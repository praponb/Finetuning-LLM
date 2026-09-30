select *
from (
    select date_format(txn_time, 'yyyy-MM') as txn_month, count(amount) as amount_count
    from transactions
    group by date_format(txn_time, 'yyyy-MM')
) as x
where amount_count < 5;
