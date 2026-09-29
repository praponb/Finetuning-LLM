select status
from subscriptions
where month(start_date) in (6, 8, 11)
union all
select status
from subscriptions
where customer_id is null;
