select
    t.txn_type,
    t.is_flagged,
    a.opened_date,
    a.balance,
    c.segment
from transactions t
inner join accounts a on a.id = t.account_id
full outer join customers c on a.customer_id = c.id
where t.amount > 1300;
