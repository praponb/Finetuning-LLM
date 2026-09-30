select
    id,
    store_id,
    product_id,
    units,
    percent_rank() over (partition by store_id, product_id order by units desc) as units_pct_rank,
    row_number() over (partition by product_id order by units, id) as row_num
from store_sales
