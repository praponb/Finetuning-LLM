select status
from shipments
where month(delivered_date) in (6, 9, 10)
union
select status
from subscriptions
where plan = 'premium'
order by status
