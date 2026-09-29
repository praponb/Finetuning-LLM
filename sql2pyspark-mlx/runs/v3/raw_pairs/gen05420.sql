select
    t.txn_type,
    t.is_flagged,
    a.balance,
    a.opened_date,
    c.country
from transactions t
inner join accounts a on a.id = t.account_id
inner join customers c on c.id = a.customer_id
where a.status is not null and t.account_id = 22
