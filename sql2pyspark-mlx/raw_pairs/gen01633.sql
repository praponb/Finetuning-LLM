select
    id,
    brand,
    cost,
    launch_date,
    tags,
    supplier_id,
    ntile(3) over (partition by supplier_id, brand order by cost, id) cost_bucket,
    last_value(cost, true) over (partition by year(launch_date), brand order by cost, id rows between unbounded preceding and current row) last_known_cost
from products
