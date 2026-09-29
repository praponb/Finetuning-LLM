select c.city, sum(case when t.txn_type = 'DEBIT' then t.amount else 0 end) as debit_amount, max(t.txn_time) as latest_txn_time
from transactions t
join accounts a on a.id = t.account_id
join customers c on a.customer_id = c.id
where a.account_type = 'CHECKING'
group by c.city
order by debit_amount
