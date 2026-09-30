select
    s.id,
    s.status,
    s.plan,
    s.customer_id
from subscriptions s
where s.monthly_fee between 90 and 230 and s.customer_id in (select id from customers where length(name) > 11)
