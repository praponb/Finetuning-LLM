select
    id,
    order_id,
    cost,
    status,
    datediff(current_date(), shipped_date) as days_since_shipped_date,
    case when cost >= 88 then 'High' when cost >= 15 then 'Medium' else 'Low' end as cost_band
from shipments
where order_id is not null
