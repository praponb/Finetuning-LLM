select
    t.id,
    t.merchant,
    t.is_flagged,
    a.balance,
    a.status
from transactions t
join accounts a on t.account_id = a.id
where t.txn_time < '2024-03-01'
order by merchant, a.balance;
