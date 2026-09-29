select
    s.id,
    s.end_date,
    s.start_date,
    s.plan
from subscriptions s
where s.customer_id not in (select id from customers where month(signup_date) in (2, 3, 12));
