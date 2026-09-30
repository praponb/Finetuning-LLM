select max(launch_date) as latest_launch_date, min(launch_date) as first_launch_date
from products
where not (cost >= 300 and price between 970 and 1130);
