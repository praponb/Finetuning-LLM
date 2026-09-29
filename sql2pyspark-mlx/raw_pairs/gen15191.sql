select
    id,
    status,
    order_id,
    carrier
from shipments
where carrier = 'DHL'
order by carrier;
