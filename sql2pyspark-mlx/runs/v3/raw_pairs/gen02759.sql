select
    id,
    shipped_date,
    delivered_date,
    status,
    date_trunc('YEAR', delivered_date) as delivered_date_period
from shipments
where status in ('LOST', 'IN_TRANSIT', 'RETURNED');
