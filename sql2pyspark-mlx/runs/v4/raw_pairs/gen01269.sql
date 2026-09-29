with base as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        date_trunc('MONTH', last_restock) as last_restock_period
    from inventory
    where last_restock > to_date('2024-03-15')
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(last_restock_period) over (partition by store_id order by last_restock_period, id) is null
                 or last_restock_period > lag(last_restock_period) over (partition by store_id order by last_restock_period, id) + interval '7' day then 1 else 0 end as is_new_session
    from base
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by store_id order by last_restock_period, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by store_id order by quantity desc) as row_num
        from sessionized
    ) t
    where row_num = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_period,
        is_new_session,
        session_no,
        row_num,
        sum(session_no) over (partition by store_id, product_id order by last_restock, id rows between unbounded preceding and current row) as running_session_no,
        ntile(3) over (partition by store_id, product_id order by quantity, id) as quantity_bucket
    from latest
),
valid as (
    -- Keep recent, valid rows
    select *, date_trunc('MONTH', last_restock_period) as last_restock_period_period
    from scored
    where session_no >= 4
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(quantity) over (order by quantity, id rows between 3 preceding and 1 preceding) as quantity_avg_3_prior,
        ntile(4) over (partition by product_id order by quantity_bucket, id) as quantity_bucket_bucket
    from valid
)
-- Grouped metrics
select
    store_id,
    approx_percentile(is_new_session, 0.95) as is_new_session_p95,
    avg(quantity) as avg_quantity,
    max(last_restock_period) as last_seen,
    round(avg(quantity_avg_3_prior), 2) as avg_quantity_avg_3_prior,
    sum(session_no) as total_session_no,
    min(running_session_no) as min_running_session_no
from with_windows
group by store_id
having total_session_no > 0
