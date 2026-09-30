select
    date_format(txn_time, 'yyyy-MM') as txn_month,
    txn_type,
    round(avg(amount), 2) as avg_amount,
    min(amount) as min_amount,
    sum(case when txn_type = 'FEE' then amount else 0 end) as fee_amount
from transactions
where merchant != 'Grab' and is_flagged
group by date_format(txn_time, 'yyyy-MM'), txn_type
having sum(case when txn_type = 'REFUND' then amount else 0 end) < 64;
