select
    id,
    order_id,
    carrier,
    cost,
    delivered_date,
    last_value(delivered_date) over (partition by order_id, carrier order by cost desc, id rows between unbounded preceding and unbounded following) as last_delivered_date
from shipments
