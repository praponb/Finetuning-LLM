select
    id,
    product_id,
    units,
    dense_rank() over (partition by product_id order by units) as units_dense_rank
from store_sales;
