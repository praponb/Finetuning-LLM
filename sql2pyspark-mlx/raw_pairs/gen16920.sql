with recent_transactions as (
    select *
    from transactions
    where txn_type = 'REFUND'
),
summary as (
    select
        is_flagged,
        to_date(txn_time) as txn_day,
        round(stddev(amount), 2) as stddev_amount,
        sum(case when txn_type = 'REFUND' then 1 else 0 end) as refund_count
    from recent_transactions
    group by is_flagged, to_date(txn_time)
)
select *
from summary
where stddev_amount < 24
order by refund_count desc
