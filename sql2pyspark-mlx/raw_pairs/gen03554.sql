select
    s.device,
    s.country,
    c.city,
    c.signup_date,
    c.age
from sessions s
join customers c on s.customer_id = c.id;
