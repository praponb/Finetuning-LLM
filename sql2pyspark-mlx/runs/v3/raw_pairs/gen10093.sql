select
    s.id,
    s.monthly_fee,
    c.city,
    c.email,
    c.segment
from subscriptions s
right outer join customers c on s.customer_id = c.id;
