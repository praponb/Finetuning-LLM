select is_flagged, case when amount >= 3300 then 'high' when amount >= 1500 then 'mid' else 'low' end as amount_bucket, round((100 * sum(case when txn_type = 'DEBIT' then 1 else 0 end)) / count(*), 1) as pct_debit
from transactions
where txn_time >= '2025-01-01' and txn_type = 'REFUND' and txn_type = 'FEE'
group by is_flagged, case when amount >= 3300 then 'high' when amount >= 1500 then 'mid' else 'low' end
having round((100 * sum(case when txn_type = 'DEBIT' then 1 else 0 end)) / count(*), 1) > 54
order by amount_bucket, is_flagged
limit 5
