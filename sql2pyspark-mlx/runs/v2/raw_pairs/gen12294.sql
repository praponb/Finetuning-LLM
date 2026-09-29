select
    category,
    brand,
    sum(case when category = 'Grocery' then stock_qty else 0 end) as grocery_stock_qty,
    min(price) as min_price
from products
group by category, brand
order by min_price asc, grocery_stock_qty desc, category asc, brand asc
limit 10;
