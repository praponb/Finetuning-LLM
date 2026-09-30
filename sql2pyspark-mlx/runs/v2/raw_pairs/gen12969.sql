select category, count(stock_qty) as stock_qty_count, max(launch_date) as last_launch_date
from products
where size(tags) > 2 and supplier_id is not null
group by category
