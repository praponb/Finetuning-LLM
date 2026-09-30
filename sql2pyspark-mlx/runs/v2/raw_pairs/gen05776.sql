select
    id,
    category,
    brand,
    launch_date,
    price,
    lead(price, 1) over (partition by category, brand order by launch_date, id) next_price
from products
