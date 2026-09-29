select
    id,
    store_id,
    order_date,
    ntile(2) over (partition by store_id order by order_date, id) quartile
from orders;
