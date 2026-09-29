select
    delivered_date,
    order_id,
    year(delivered_date) as delivered_date_year,
    lower(status) as status_lower
from shipments
order by delivered_date;
