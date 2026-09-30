select
    a.id,
    a.account_type,
    a.customer_id,
    c.signup_date
from accounts a
join customers c on a.customer_id = c.id
where a.status not in ('FROZEN', 'OPEN', 'CLOSED');
