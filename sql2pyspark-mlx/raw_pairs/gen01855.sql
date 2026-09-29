select
    s.started_at,
    s.customer_id,
    s.device,
    c.signup_date,
    c.country,
    c.email
from sessions s
left outer join customers c on s.customer_id = c.id;
