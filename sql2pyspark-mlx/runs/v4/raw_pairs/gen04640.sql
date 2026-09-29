select
    category,
    avg(price) avg_price,
    sum(case when category = 'Sports' then stock_qty else 0 end) sports_stock_qty,
    count(case when category = 'Fashion' then 1 end) fashion_count
from products
group by category;
