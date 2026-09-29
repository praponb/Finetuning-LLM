select
    id,
    cost,
    product_name,
    stock_qty + cost stock_qty_plus_cost
from products
where launch_date between '2024-03-12' and '2024-07-01' and supplier_id = 38
order by stock_qty_plus_cost desc, id desc;
