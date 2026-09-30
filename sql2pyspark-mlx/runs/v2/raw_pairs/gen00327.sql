select
    a.account_type,
    a.status,
    c.city,
    c.signup_date
from accounts a
inner join customers c on a.customer_id = c.id
order by a.status
