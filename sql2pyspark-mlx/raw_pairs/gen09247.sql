select
    id,
    store_id,
    quantity,
    avg(quantity) over (partition by store_id order by quantity asc, id asc rows between 3 preceding and current row) as quantity_avg_3,
    ntile(4) over w_grp as quantity_bucket,
    ntile(10) over (partition by product_id order by quantity asc, id asc) as quantity_bucket_2,
    lead(quantity, 2) over (partition by store_id order by last_restock asc, id asc) as next_quantity
from inventory
window w_grp as (partition by store_id order by quantity desc, id asc);
