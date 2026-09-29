select *, replace(supplier_name, ' ', '') as supplier_name_slug, concat_ws('/', supplier_name, city) as supplier_name_city_key
from suppliers
