select
    id,
    shipped_date,
    delivered_date,
    order_id
from shipments
where shipped_date = (select max(shipped_date) from shipments);
