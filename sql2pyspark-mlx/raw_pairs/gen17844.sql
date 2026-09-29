with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        date_trunc('MONTH', txn_time) as txn_time_period,
        day(txn_time) as txn_time_day
    from transactions
    where merchant is not null
      and amount >= 3100
),
with_dims as (
    -- Attach accounts attributes
    select src.*, b.status
    from filtered src
    left join accounts b
        on src.account_id = b.id
),
valid as (
    -- Keep recent, valid rows
    select *, cast(is_flagged as int) as is_flagged_flag
    from with_dims
    where account_id in (4, 14, 21)
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        txn_time_period,
        txn_time_day,
        status,
        is_flagged_flag,
        row_number() over (partition by txn_type order by txn_time_period, id) as row_seq,
        last_value(amount, true) over (partition by year(txn_time_period), account_id order by txn_time_period, id rows between unbounded preceding and current row) as last_known_amount
    from valid
)
-- Multi-level aggregation
select
    date_trunc('month', txn_time_period) as activity_month,
    sort_array(collect_set(status)) as status_list,
    round(sum(row_seq) / nullif(count(case when txn_type = 'FEE' then 1 end), 0), 2) as row_seq_per_fee
from scored
group by activity_month
order by row_seq_per_fee desc
