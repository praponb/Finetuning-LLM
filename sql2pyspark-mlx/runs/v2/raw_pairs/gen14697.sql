select
    s.id,
    s.customer_id,
    c.signup_date,
    c.email
from sessions s
full outer join customers c on s.customer_id = c.id;
