select plan
from subscriptions
union all
select country as plan
from customers;
