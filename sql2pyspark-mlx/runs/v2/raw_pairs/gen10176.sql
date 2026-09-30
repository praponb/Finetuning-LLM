select
    id,
    brand,
    stock_qty,
    category,
    supplier_id,
    cost,
    product_name,
    min(stock_qty) over (partition by brand) as brand_min_stock_qty,
    last_value(product_name) over (partition by category, supplier_id order by cost, id rows between unbounded preceding and unbounded following) as last_product_name
from products
where cost >= 230
