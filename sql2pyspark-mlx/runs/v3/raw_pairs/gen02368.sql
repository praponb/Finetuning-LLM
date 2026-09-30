select
    s.id,
    s.country,
    c.segment,
    c.name,
    c.signup_date,
    datediff(current_date(), s.started_at) days_since_started_at
from sessions s
full outer join customers c on s.customer_id = c.id;
