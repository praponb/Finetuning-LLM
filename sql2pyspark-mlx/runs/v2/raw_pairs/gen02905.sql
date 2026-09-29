select
    a.id,
    a.opened_date,
    a.status,
    a.account_type,
    c.city,
    c.credit_limit
from accounts a
inner join customers c on a.customer_id = c.id
where c.is_active
