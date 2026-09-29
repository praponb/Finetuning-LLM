select
    id,
    brand,
    cost,
    dense_rank() over (partition by brand order by cost) as dense_rnk,
    round(cost / sum(cost) over (partition by brand), 3) as cost_share
from products;
