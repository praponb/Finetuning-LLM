with filtered as (
    -- Basic cleansing and defaults
    select *, date_trunc('year', last_restock) as last_restock_period
    from inventory
    where last_restock >= to_date('2023-08-12')
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(last_restock_period) over (partition by store_id order by last_restock_period, id) is null
                 or last_restock_period > lag(last_restock_period) over (partition by store_id order by last_restock_period, id) + interval 14 days then 1 else 0 end as is_new_session
    from filtered
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock_period,
        sum(is_new_session) over (partition by store_id order by last_restock_period, id rows between unbounded preceding and current row) as session_id
    from flagged
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by store_id, product_id order by last_restock_period desc, id) as latest_rank
    from sessionized
    qualify latest_rank = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by store_id order by quantity desc, id) as event_seq,
        cume_dist() over (partition by date_trunc('MONTH', last_restock_period), store_id order by session_id desc) as session_id_cume_dist,
        percent_rank() over (partition by store_id order by session_id desc) as session_id_percent_rank,
        avg(quantity) over (partition by product_id order by cast(last_restock_period as bigint) range between 604800 preceding and current row) as quantity_avg_7d,
        ntile(10) over (order by quantity, id) as quantity_bucket
    from deduped
    where last_restock_period < '2024-05-01'
)
select
    id,
    quantity,
    last_restock_period,
    session_id,
    latest_rank,
    event_seq,
    session_id_cume_dist,
    session_id_percent_rank,
    quantity_avg_7d
from with_windows;
