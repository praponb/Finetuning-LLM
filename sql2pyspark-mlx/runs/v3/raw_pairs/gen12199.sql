select
    a.balance,
    a.status,
    a.account_type,
    c.segment,
    c.country
from accounts a
left outer join customers c on a.customer_id = c.id
where a.customer_id in (9, 24, 34)
