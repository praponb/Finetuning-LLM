select id, start_date, lower(status) as status_lower
from subscriptions
where customer_id is not null
order by 2 desc
