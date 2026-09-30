select
    supplier_id,
    brand,
    cost + stock_qty as cost_plus_stock_qty,
    upper(product_name) as product_name_upper
from products
where supplier_id = 35 and brand in ('Stark', 'Acme', 'Globex') and cost between 300 and 330
