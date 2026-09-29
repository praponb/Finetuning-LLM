select
    txn_type,
    txn_month,
    first_txn_time,
    row_count
from (
    select
        txn_type,
        date_format(txn_time, 'yyyy-MM') as txn_month,
        min(txn_time) as first_txn_time,
        count(*) as row_count
    from transactions
    group by txn_type, date_format(txn_time, 'yyyy-MM')
) as s
where row_count < 6
order by txn_month, row_count desc, txn_type
limit 20
