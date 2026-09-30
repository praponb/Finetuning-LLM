with valid_transactions as (
    select *
    from transactions
    where txn_type = 'REFUND' and account_id = 36 and txn_type = 'REFUND'
),
txn_month_summary as (
    select
        month(txn_time) as txn_month,
        txn_type,
        avg(amount) as avg_amount,
        count(*) as cnt
    from valid_transactions
    group by month(txn_time), txn_type
)
select
    txn_month,
    txn_type,
    avg_amount,
    cnt
from txn_month_summary;
