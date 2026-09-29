select end_date, 'subscription' source
from subscriptions
union all
select signup_date end_date, 'customer' source
from customers
where age <= 35
order by source desc;
