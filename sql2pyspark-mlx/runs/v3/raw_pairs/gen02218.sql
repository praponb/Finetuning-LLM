select carrier, status
from shipments
where delivered_date >= '2024-01-01'
union all
select carrier, status
from shipments
where cost < 41;
