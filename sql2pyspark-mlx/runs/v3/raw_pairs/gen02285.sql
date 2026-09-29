select brand, sum(case when category = 'Sports' then stock_qty else 0 end) as sports_stock_qty
from products
group by 1
having sports_stock_qty < 27
