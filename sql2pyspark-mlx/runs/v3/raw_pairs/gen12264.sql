select
    id,
    product_id,
    quantity,
    percent_rank() over (partition by product_id order by quantity desc) quantity_pct_rank,
    dense_rank() over (partition by product_id order by quantity desc) quantity_dense_rank
from inventory;
