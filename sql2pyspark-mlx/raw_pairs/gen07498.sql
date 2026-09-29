select
    s.id,
    s.monthly_fee,
    s.plan,
    c.segment,
    c.email,
    c.city
from subscriptions s
full outer join customers c on c.id = s.customer_id;
