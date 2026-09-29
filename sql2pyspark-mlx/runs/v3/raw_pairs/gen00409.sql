select start_date
from subscriptions
union
select shipped_date as start_date
from shipments
where delivered_date > '2024-10-01';
