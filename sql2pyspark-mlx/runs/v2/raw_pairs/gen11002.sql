select
    t.id,
    t.account_id,
    a.account_type,
    c.is_active,
    c.age
from transactions t
join accounts a on t.account_id = a.id
right join customers c on a.customer_id = c.id
where a.status = 'OPEN' and month(c.signup_date) in (5, 8, 10)
