select carrier, shipped_date, delivered_date
from shipments
where status != 'LOST' and carrier in ('FedEx', 'Flash') and order_id = 8
