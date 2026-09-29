select
    id,
    store_id,
    sale_date,
    product_id,
    units,
    rank() over (partition by store_id order by sale_date) as sale_date_rank,
    round(units / sum(units) over (partition by product_id), 3) as units_share
from store_sales;
