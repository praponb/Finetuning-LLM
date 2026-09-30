select id, status
from subscriptions
where year(start_date) = 2023
union
select id, status
from subscriptions
where customer_id in (12, 25, 36, 39)
