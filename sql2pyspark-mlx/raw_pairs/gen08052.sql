select shipped_date, carrier
from shipments
where shipped_date < '2024-12-06'
intersect
select shipped_date, carrier
from shipments
where cost < 91;
