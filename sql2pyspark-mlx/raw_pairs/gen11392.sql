with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(quantity, 0) quantity_filled
    from inventory
    where last_restock >= to_date('2023-03-31')
),
base as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        last_restock,
        add_months(last_restock, 3) last_restock_plus_months,
        date_trunc('year', last_restock) last_restock_period
    from cleaned
    where quantity_filled between 36 and 59
),
with_dims as (
    -- Attach products attributes
    select a.*, d.product_name
    from base a
    left join products d
        on a.product_id = d.id
),
with_prev as (
    -- Previous event time per key
    select *, lag(last_restock_period) over (partition by product_id order by last_restock_period, id) as prev_last_restock_period
    from with_dims
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_last_restock_period is null or last_restock_period > prev_last_restock_period + interval 30 days then 1 else 0 end as is_new_session
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        product_id,
        last_restock,
        last_restock_period,
        sum(is_new_session) over (partition by product_id order by last_restock_period, id rows between unbounded preceding and current row) as session_no
    from with_gaps
)
-- Multi-level aggregation
select
    date_trunc('year', last_restock) as year_start,
    max(session_no) as max_session_no,
    min(last_restock_period) as first_last_restock_period,
    (unix_timestamp(max(last_restock_period)) - unix_timestamp(min(last_restock_period))) / 60 as duration_min
from islands
group by date_trunc('year', last_restock)
having (unix_timestamp(max(last_restock_period)) - unix_timestamp(min(last_restock_period))) / 60 > 0
order by max_session_no desc nulls last
