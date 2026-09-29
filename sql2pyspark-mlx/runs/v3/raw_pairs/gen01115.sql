select
    brand,
    category,
    max(stock_qty) as max_stock_qty,
    count(*) as cnt
from products
group by brand, category
having max(stock_qty) < 340
