select
    id,
    supplier_id,
    brand,
    cost,
    length(product_name) as product_name_length,
    round(stock_qty, 2) as stock_qty_rounded
from products
where category = 'Sports'
order by brand, 2 desc;
