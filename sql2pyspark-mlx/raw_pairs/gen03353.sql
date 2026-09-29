select case when amount >= 3600 then 'high' when amount >= 1000 then 'mid' else 'low' end as amount_bucket, max(txn_time) as latest_txn_time, avg(amount) as avg_amount
from transactions
where (txn_type = 'REFUND' and is_flagged) or account_id in (1, 16)
group by amount_bucket
having avg_amount >= 2100
order by amount_bucket desc
limit 5
