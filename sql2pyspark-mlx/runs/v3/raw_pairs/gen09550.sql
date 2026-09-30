select c.is_active, max(t.amount) - min(t.amount) as amount_range, count(case when t.txn_type = 'DEBIT' then 1 end) as debit_count
from transactions t
right join accounts a on t.account_id = a.id
join customers c on c.id = a.customer_id
group by c.is_active
having max(t.amount) - min(t.amount) > 38
