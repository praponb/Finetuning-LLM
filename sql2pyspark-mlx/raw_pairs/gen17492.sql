select
    s.monthly_fee,
    s.status,
    s.plan,
    c.credit_limit,
    c.is_active,
    c.age,
    concat(c.name, ' ', c.segment) as name_segment
from subscriptions s
inner join customers c on s.customer_id = c.id;
