select *, lower(supplier_name) as supplier_name_lower, replace(supplier_name, ' ', '-') as supplier_name_slug
from suppliers
where country = 'US' and country <> 'JP'
