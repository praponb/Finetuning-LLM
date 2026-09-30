select supplier_id, concat_ws(', ', sort_array(collect_set(brand))) as brand_values
from products
group by supplier_id
