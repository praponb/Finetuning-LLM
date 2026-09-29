select category, sum(stock_qty) as total_stock_qty, dense_rank() over (order by sum(stock_qty) desc) as rnk
from products
group by category
