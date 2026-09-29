select
    id,
    store_id,
    units,
    ntile(3) over (partition by store_id order by units desc, id) as quartile
from store_sales;
