select id, start_date, end_date
from subscriptions
where monthly_fee <= 200
order by id desc;
