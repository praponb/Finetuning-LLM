select category, min(launch_date) min_launch_date
from products
group by category
