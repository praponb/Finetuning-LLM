with filtered as (
    -- Basic cleansing and defaults
    select *, date_trunc('YEAR', sale_date) as sale_date_period
    from store_sales
    where sale_date > to_date('2023-03-21')
),
joined as (
    -- Attach stores attributes
    select
        src.id,
        src.store_id,
        src.product_id,
        src.sale_date,
        src.units,
        src.sale_date_period,
        b.store_name
    from filtered src
    left join stores b
        on src.store_id = b.id
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(sale_date_period) over (partition by store_id order by sale_date_period, id) is null
                 or sale_date_period > lag(sale_date_period) over (partition by store_id order by sale_date_period, id) + interval '7' day then 1 else 0 end as is_new_session
    from joined
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        store_id,
        sale_date,
        units,
        sale_date_period,
        store_name,
        sum(is_new_session) over (partition by store_id order by sale_date_period, id rows between unbounded preceding and current row) as session_id
    from flagged
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        avg(units) over (partition by store_id order by session_id, id rows between 6 preceding and 1 preceding) as units_avg_6_prior,
        sum(units) over (order by cast(sale_date_period as bigint) range between 2592000 preceding and current row) as units_sum_30d
    from islands
),
filtered_store_sales as (
    -- Basic cleansing and defaults
    select
        id,
        sale_date,
        units,
        store_name,
        session_id,
        units_avg_6_prior,
        units_sum_30d,
        to_date(sale_date_period) as sale_date_period_date
    from with_windows
    where sale_date > date '2023-07-17'
)
-- Aggregate with conditional metrics
select
    session_id,
    case when units_avg_6_prior >= 35 then 'HIGH' else 'LOW' end as units_avg_6_prior_tier,
    count(distinct id) as distinct_ids,
    bit_and(units) as units_bit_and,
    max(units_avg_6_prior) as max_units_avg_6_prior,
    count(*) as cnt,
    grouping(session_id) as grouping_id
from filtered_store_sales
group by rollup (session_id, case when units_avg_6_prior >= 35 then 'HIGH' else 'LOW' end)
order by max_units_avg_6_prior nulls last;
