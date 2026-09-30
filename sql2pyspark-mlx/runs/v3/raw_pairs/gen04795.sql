select
    s.id,
    s.start_date,
    s.monthly_fee,
    s.customer_id,
    c.segment,
    c.country,
    c.email
from subscriptions s
inner join customers c on s.customer_id = c.id
order by email;
