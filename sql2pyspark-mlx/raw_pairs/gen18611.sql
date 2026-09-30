select category, max(launch_date) as last_launch_date, min(price) as min_price
from products
group by category
having min_price > 380;
