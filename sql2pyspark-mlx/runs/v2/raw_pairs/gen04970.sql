select
    id,
    order_id,
    cost,
    avg(cost) over (partition by order_id) as order_id_avg_cost
from shipments
where carrier not in ('UPS', 'Kerry', 'Flash')
order by order_id_avg_cost desc
