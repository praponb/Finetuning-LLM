select a.account_type, sum(case when t.merchant = 'Amazon' then t.amount else 0 end) as amazon_amount, sum(case when t.txn_type = 'REFUND' then t.amount else 0 end) as refund_amount
from transactions t
inner join accounts a on t.account_id = a.id
where t.txn_time < '2024-10-01'
group by a.account_type
having sum(case when t.txn_type = 'REFUND' then t.amount else 0 end) >= 39
order by amazon_amount
