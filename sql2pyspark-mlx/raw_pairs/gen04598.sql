with latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by merchant order by txn_time nulls first, id) as rn
        from transactions
    ) ranked
    where rn = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by merchant order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by merchant order by txn_time, id) + interval '60' day then 1 else 0 end as session_start_flag
    from latest
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
enriched as (
    -- Attach accounts attributes
    select
        a.*,
        p.status,
        p.opened_date,
        p.balance
    from sessionized a
    left join accounts p
        on a.account_id = p.id
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        txn_time,
        merchant,
        session_start_flag,
        burst_id,
        status,
        opened_date,
        balance,
        avg(burst_id) over (partition by status order by cast(txn_time as bigint) range between 2592000 preceding and current row) as burst_id_avg_30d,
        sum(amount) over w_ordered as cumulative_amount
    from enriched
    window w_ordered as (partition by to_date(txn_time), txn_type order by rn, id rows between unbounded preceding and current row)
),
daily as (
    -- Grouped metrics
    select
        burst_id,
        date_trunc('week', opened_date) as week_start,
        count(*) filter (where merchant = 'Shell') as shell_rows,
        max(txn_time) as last_seen,
        (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as span_minutes,
        count(case when merchant = '7-Eleven' then 1 end) as 7_eleven_count,
        grouping_id() as gid
    from scored
    group by grouping sets ((burst_id, date_trunc('week', opened_date)), (burst_id))
)
select
    last_seen,
    span_minutes,
    7_eleven_count,
    gid
from daily
order by span_minutes desc
