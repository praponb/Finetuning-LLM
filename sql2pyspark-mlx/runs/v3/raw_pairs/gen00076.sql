select
    t.account_id,
    a.status,
    a.balance,
    a.opened_date,
    round(t.amount, 0) as amount_rounded
from transactions t
inner join accounts a on t.account_id = a.id
where hour(t.txn_time) between 10 and 13 and a.customer_id = 7
order by opened_date desc
