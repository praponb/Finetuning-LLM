select
    id,
    launch_date,
    cost,
    price
from products
where launch_date = (select max(launch_date) from products)
