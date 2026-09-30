select
    id,
    status,
    delivered_date,
    row_number() over (partition by status order by delivered_date desc, id) as delivered_date_row
from shipments
