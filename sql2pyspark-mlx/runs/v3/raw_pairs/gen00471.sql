select
    id,
    brand,
    stock_qty,
    upper(product_name) product_name_upper
from products
where price > 1100 and launch_date <= '2023-03-27' and array_contains(tags, 'clearance')
order by product_name_upper, id;
