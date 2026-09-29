select
    id,
    supplier_id,
    launch_date,
    product_name,
    last_value(product_name) over (partition by supplier_id order by launch_date, id rows between unbounded preceding and unbounded following) as last_product_name
from products
where price > 790
order by product_name, launch_date desc
