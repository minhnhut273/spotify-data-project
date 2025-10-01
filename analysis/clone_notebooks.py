import nbformat
import os

# Nguồn gốc notebook (template)
template_file = "analysis/EDA_france.ipynb"

# Danh sách quốc gia (folder data)
countries = [
    "france", "italy", "japan", "mexico", "south-korea",
    "spain", "uk", "usa", "argentina", "world"
]

# Tạo thư mục analysis nếu chưa có
os.makedirs("analysis", exist_ok=True)

# Đọc notebook gốc
with open(template_file, "r", encoding="utf-8") as f:
    nb = nbformat.read(f, as_version=4)

# Lặp từng quốc gia để clone
for country in countries:
    nb_copy = nbformat.from_dict(nb)  # copy cấu trúc
    
    # Thay chữ 'France' trong markdown/code thành tên nước tương ứng
    for cell in nb_copy.cells:
        if cell.cell_type in ["markdown", "code"]:
            cell.source = cell.source.replace("France", country.capitalize())
            cell.source = cell.source.replace("france", country)  # lowercase cho path
    
    # Ghi file mới vào folder analysis
    out_file = os.path.join("analysis", f"EDA_{country}.ipynb")
    with open(out_file, "w", encoding="utf-8") as f:
        nbformat.write(nb_copy, f)
    
    print(f" Created {out_file}")
