with base as (
    -- Basic cleansing and defaults
    select *, ceil(revenue) as revenue_whole
    from store_sales
    where sale_date >= date '2023-08-14'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        units,
        revenue,
        revenue_whole,
        round(revenue / units, 2) as revenue_per_unit,
        date_trunc('year', sale_date) as sale_date_period
    from base
    where year(sale_date) >= 2024
),
metrics as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        units,
        revenue,
        revenue_whole,
        revenue_per_unit,
        sale_date_period,
        avg(revenue_per_unit) over (partition by date_trunc('week', sale_date_period) order by cast(sale_date_period as bigint) range between 604800 preceding and current row) as revenue_per_unit_avg_7d,
        row_number() over (order by revenue_per_unit desc, id) as row_seq
    from cleaned
),
with_dims as (
    -- Attach stores attributes
    select
        src.*,
        s.opened_date,
        s.store_name
    from metrics src
    inner join stores s
        on src.store_id = s.id
),
valid as (
    -- Basic cleansing and defaults
    select *, upper(store_name) as store_name_upper
    from with_dims
    where revenue_per_unit_avg_7d < 23
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when sale_date_period - lag(sale_date_period) over (partition by product_id order by sale_date_period, id) > interval 7 days then 1 else 0 end as new_group_flag
    from valid
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by product_id order by sale_date_period, id rows between unbounded preceding and current row) as session_no
    from flagged
),
metrics_store_sales as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        units,
        revenue,
        revenue_whole,
        sale_date_period,
        revenue_per_unit_avg_7d,
        opened_date,
        new_group_flag,
        min(units) over (partition by store_id order by opened_date, id rows between unbounded preceding and current row) as running_min_units,
        avg(units) over (partition by store_id order by opened_date, id rows between unbounded preceding and current row) as running_avg_units,
        sum(units) over (partition by store_id order by opened_date, id rows between unbounded preceding and current row) as running_sum_units,
        row_number() over (partition by date_trunc('week', opened_date), product_id order by session_no desc, id) as row_seq_2
    from islands
)
select
    s.id,
    s.store_id,
    s.revenue,
    s.revenue_whole,
    s.revenue_per_unit_avg_7d,
    s.opened_date,
    s.new_group_flag,
    s.running_min_units,
    s.running_avg_units,
    (select avg(t2.units) from store_sales t2 where t2.store_id = s.store_id) as avg_store_sale_units
from metrics_store_sales s
order by id desc nulls last, avg_store_sale_units desc
