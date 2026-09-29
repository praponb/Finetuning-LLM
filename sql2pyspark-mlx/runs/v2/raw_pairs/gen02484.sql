select
    id,
    order_id,
    delivered_date,
    round(cost * 100, 2) as cost_scaled
from shipments
