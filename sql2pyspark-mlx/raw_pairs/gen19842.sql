select
    a.customer_id,
    a.opened_date,
    c.credit_limit,
    c.email
from accounts a
inner join customers c on a.customer_id = c.id
where c.signup_date < '2025-08-01' and month(a.opened_date) in (1, 2, 6)
