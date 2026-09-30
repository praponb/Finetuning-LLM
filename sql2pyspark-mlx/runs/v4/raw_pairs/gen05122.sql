with prev_events as (
    select *, lag(txn_time) over (partition by account_id order by txn_time, id) as prev_txn_time
    from transactions
),
gaps as (
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval '90' day then 1 else 0 end as is_new_burst
    from prev_events
),
islands as (
    select *, sum(is_new_burst) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as island_id
    from gaps
)
select
    date_trunc('week', txn_time) as week_start,
    island_id,
    avg(is_new_burst) as avg_is_new_burst,
    count(case when merchant = 'Lazada' then 1 end) as lazada_count,
    max(island_id) as max_island_id
from islands
group by date_trunc('week', txn_time), island_id
order by avg_is_new_burst;
