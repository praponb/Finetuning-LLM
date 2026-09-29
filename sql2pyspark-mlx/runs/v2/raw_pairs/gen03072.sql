select
    a.id,
    a.opened_date,
    a.status,
    c.country,
    c.credit_limit,
    c.age
from accounts a
inner join customers c on a.customer_id = c.id
where a.account_type = 'SAVINGS';
