select
    id,
    tags,
    brand,
    cost,
    datediff(current_date(), launch_date) / 30 months_since_launch_date
from products
where price is not null or stock_qty <= 176
