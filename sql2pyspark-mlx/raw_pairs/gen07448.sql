select
    id,
    store_id,
    units,
    round(units / sum(units) over (partition by store_id), 3) as units_share
from store_sales;
