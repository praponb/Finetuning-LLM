select
    delivered_date,
    status,
    length(status) as status_length,
    lower(status) as status_lower
from shipments
where order_id is not null and order_id is not null
order by status
