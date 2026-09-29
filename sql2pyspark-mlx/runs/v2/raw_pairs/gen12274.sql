select
    id,
    product_id,
    quantity,
    rank() over (partition by product_id order by quantity desc) as rnk
from inventory;
