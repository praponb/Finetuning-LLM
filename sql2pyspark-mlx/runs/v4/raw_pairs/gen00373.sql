select concat_ws(', ', sort_array(collect_set(country))) as country_values
from suppliers
where rating >= 2.1 and country = 'DE' and supplier_name like '%Siam%'
