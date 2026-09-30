select
    s.id,
    s.country,
    s.started_at,
    c.name,
    c.signup_date,
    c.segment
from sessions s
join customers c on s.customer_id = c.id
where s.browser = 'Chrome';
