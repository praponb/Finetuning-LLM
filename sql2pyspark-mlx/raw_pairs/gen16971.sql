select
    s.plan,
    c.age,
    c.is_active,
    c.signup_date
from subscriptions s
join customers c on s.customer_id = c.id
where c.is_active = true;
