select
    a.account_type,
    a.status,
    min(t.txn_time) as min_txn_time,
    round(avg(t.amount), 2) as avg_amount
from transactions t
full outer join accounts a on t.account_id = a.id
where t.merchant = 'Shell'
group by a.account_type, a.status
order by status, avg_amount desc;
