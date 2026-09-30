select distinct plan
from subscriptions
where month(end_date) in (3, 7, 8)
order by plan asc
limit 20
