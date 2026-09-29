select
    id,
    shipped_date,
    delivered_date,
    datediff(delivered_date, shipped_date) as days_shipped_date_to_delivered_date
from shipments;
