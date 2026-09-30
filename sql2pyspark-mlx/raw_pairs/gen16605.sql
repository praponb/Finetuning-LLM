select distinct plan
from subscriptions
where month(start_date) in (2, 4, 7)
order by plan
