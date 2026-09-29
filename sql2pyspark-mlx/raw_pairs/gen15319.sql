select
    c.id,
    c.signup_date,
    c.name,
    c.country
from customers c
where not exists (select 1 from subscriptions s where s.customer_id = c.id)
