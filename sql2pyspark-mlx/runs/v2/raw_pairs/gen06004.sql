select id, end_date
from subscriptions
where end_date < (select min(end_date) from subscriptions)
order by 2 asc;
