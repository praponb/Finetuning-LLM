select end_date, start_date
from subscriptions
where plan in ('premium', 'basic')
intersect
select end_date, start_date
from subscriptions
where end_date < '2023-10-01'
order by start_date
