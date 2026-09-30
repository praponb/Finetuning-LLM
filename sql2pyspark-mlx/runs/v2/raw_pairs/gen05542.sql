select category, sum(stock_qty) as total_stock_qty, max(cost) - min(cost) as cost_range
from products
where cost < 460 and stock_qty > 206
group by 1
order by total_stock_qty desc, cost_range desc, category
limit 20;
