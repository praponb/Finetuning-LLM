select
    id,
    category,
    price,
    round(price / sum(price) over (partition by category), 3) price_share
from products
order by category
