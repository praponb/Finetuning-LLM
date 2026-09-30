select id, monthly_fee, start_date
from subscriptions
where monthly_fee >= (select avg(monthly_fee) from subscriptions)
order by start_date, id
limit 1
