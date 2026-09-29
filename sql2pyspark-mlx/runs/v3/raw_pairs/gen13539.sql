select
    id,
    brand,
    cost,
    category,
    price,
    dense_rank() over (partition by brand order by cost) as dense_rnk,
    round(price / sum(price) over (partition by category), 3) as price_share
from products;
