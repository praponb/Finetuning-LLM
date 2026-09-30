with src as (
    select *
    from transactions
    where txn_type = 'REFUND' or account_id in (1, 15)
),
TxnTypeSummary as (
    select txn_type, round((100 * sum(case when txn_type = 'FEE' then 1 else 0 end)) / count(*), 1) pct_fee, count(case when txn_type = 'REFUND' then 1 end) refund_count
    from src
    group by txn_type
)
select *
from TxnTypeSummary
