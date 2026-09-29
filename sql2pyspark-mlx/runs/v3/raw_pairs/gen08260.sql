select status, cost, order_id
from shipments
where carrier in ('Kerry', 'DHL') and month(delivered_date) in (3, 4, 5)
