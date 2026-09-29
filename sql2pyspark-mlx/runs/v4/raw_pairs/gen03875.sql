select category, sum(stock_qty) total_stock_qty, count(*) cnt, rank() over (order by sum(stock_qty) desc) as rnk from products group by category;
