select launch_date
from products
where year(launch_date) = 2024
union
select launch_date
from products
where stock_qty <= 226;
