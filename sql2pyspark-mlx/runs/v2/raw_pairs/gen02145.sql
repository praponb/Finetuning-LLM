select id, city, lower(supplier_name) as supplier_name_lower
from suppliers
where rating >= 3.8 and country = 'JP'
order by city desc
