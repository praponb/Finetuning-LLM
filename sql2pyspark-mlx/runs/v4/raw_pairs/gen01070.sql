select *
from (
    select quarter(txn_time) as txn_quarter, sum(case when txn_type = 'DEBIT' then amount else 0 end) as debit_amount, count(amount) as amount_count
    from transactions
    group by quarter(txn_time)
) as t
where debit_amount >= 72
order by amount_count, txn_quarter desc;
