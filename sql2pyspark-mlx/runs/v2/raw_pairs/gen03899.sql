select brand, sort_array(collect_set(brand)) as brand_list
from products
where supplier_id in (28, 36) and stock_qty > 158
group by brand
