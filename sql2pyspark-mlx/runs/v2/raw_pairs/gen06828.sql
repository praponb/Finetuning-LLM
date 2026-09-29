select id, delivered_date
from shipments
where delivered_date = (select min(delivered_date) from shipments);
