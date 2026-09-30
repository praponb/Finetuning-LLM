select
    id,
    carrier,
    order_id,
    concat_ws('-', status, carrier) as status_carrier_key
from shipments
where carrier = 'Kerry' and shipped_date between '2025-05-01' and '2025-08-01';
