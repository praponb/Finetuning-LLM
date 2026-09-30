select round((100 * sum(case when txn_type = 'REFUND' then 1 else 0 end)) / count(*), 1) as pct_refund, avg(amount) as avg_amount, count(case when merchant = 'Amazon' then 1 end) as amazon_count
from transactions
where not (txn_type = 'DEBIT' and is_flagged = true and is_flagged = true)
order by amazon_count desc, pct_refund desc;
