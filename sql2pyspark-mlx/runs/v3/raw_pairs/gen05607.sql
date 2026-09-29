select
    t.id,
    t.is_flagged,
    t.txn_time,
    a.account_type,
    a.balance,
    c.email
from transactions t
join accounts a on a.id = t.account_id
join customers c on a.customer_id = c.id
where t.txn_type not in ('REFUND', 'CREDIT', 'FEE')
order by balance desc;
