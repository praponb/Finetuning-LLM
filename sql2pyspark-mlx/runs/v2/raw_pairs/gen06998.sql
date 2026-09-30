select count(stock_qty) as stock_qty_count
from products
order by stock_qty_count desc
