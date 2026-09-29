with base as (
    select *, cast(is_flagged as int) as is_flagged_flag
    from transactions
    where account_id in (6, 10, 36, 39)
),
transactions_stats as (
    select date_trunc('MONTH', txn_time) as txn_month, round(avg(amount), 2) as avg_amount
    from base
    group by date_trunc('MONTH', txn_time)
)
select *
from transactions_stats
where avg_amount > 44
order by txn_month;
