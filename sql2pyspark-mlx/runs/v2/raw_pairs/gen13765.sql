select
    id,
    shipped_date,
    delivered_date,
    status
from shipments
where shipped_date > (select max(shipped_date) from shipments);
