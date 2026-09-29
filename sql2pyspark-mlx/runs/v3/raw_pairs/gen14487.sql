select
    id,
    carrier,
    cost,
    order_id,
    date_format(delivered_date, 'yyyy-MM-dd') as delivered_date_label,
    round(cost, 1) as cost_rounded
from shipments
order by order_id desc, cost_rounded, id
limit 50
