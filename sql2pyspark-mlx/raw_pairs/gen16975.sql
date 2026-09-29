with filtered as (
    -- Keep recent, valid rows
    select *, cast(is_flagged as int) as is_flagged_flag
    from transactions
    where account_id in (6, 20)
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        case when merchant = '7-Eleven' then 1 else 0 end as is_7_eleven,
        coalesce(merchant, 'DIRECT') as merchant_clean
    from filtered
    where merchant is not null
      and merchant = 'Grab'
),
enriched as (
    -- Attach accounts attributes
    select
        s.*,
        b.opened_date,
        b.status
    from cleaned s
    left join accounts b
        on s.account_id = b.id
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by merchant order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by merchant order by txn_time, id) + interval '90' day then 1 else 0 end as new_group_flag
    from enriched
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_flag,
        is_7_eleven,
        merchant_clean,
        sum(new_group_flag) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(current_date(), txn_time) as days_since_txn_time
    from islands
    where txn_time >= timestamp '2023-06-02 10:00:00'
      and is_flagged_flag between 24 and 40
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        txn_type,
        txn_time,
        is_7_eleven,
        merchant_clean,
        burst_id,
        days_since_txn_time
    from (
        select *, row_number() over (partition by merchant, txn_type order by burst_id desc, id) as rank_in_group
        from base
    ) x
    where rank_in_group = 1
)
-- Multi-level aggregation
select
    date_trunc('quarter', txn_time) as quarter_start,
    burst_id,
    max(txn_time) as last_txn_time,
    approx_percentile(is_7_eleven, array(0.25, 0.5, 0.75)) as is_7_eleven_quartiles,
    avg(is_7_eleven) filter (where txn_type = 'DEBIT') as debit_avg_is_7_eleven,
    max(txn_time) as ended_at,
    max(txn_time) as ended_at_2,
    round(avg(is_7_eleven), 2) as avg_is_7_eleven
from latest
group by quarter_start, burst_id
order by debit_avg_is_7_eleven desc
