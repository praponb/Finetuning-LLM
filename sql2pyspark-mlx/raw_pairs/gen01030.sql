with valid as (
    -- Filter the raw rows and derive helper columns
    select *, last_day(sale_date) as sale_date_month_end
    from store_sales
    where sale_date >= to_date('2023-02-25')
      and sale_date < '2023-04-13'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        first_value(revenue) ignore nulls over (order by sale_date_month_end, id rows between unbounded preceding and current row) as first_known_revenue,
        rank() over (partition by product_id order by units desc nulls last) as units_rank,
        row_number() over (partition by product_id order by sale_date_month_end desc, id) as seq_no
    from valid
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        units_rank,
        cast(first_known_revenue as int) as first_known_revenue_int
    from with_windows
    where revenue <= 2900
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from filtered
    qualify rank() over (partition by store_id, product_id order by sale_date desc nulls last) = 1
),
enriched as (
    -- Ranking and gap features
    select
        *,
        first_value(units) ignore nulls over w_grp as first_known_units,
        avg(first_known_revenue_int) over w_grp_2 as first_known_revenue_int_avg_6_prior,
        avg(first_known_revenue_int) over (partition by store_id order by units_rank, id rows between 3 preceding and 1 preceding) as first_known_revenue_int_avg_3_prior
    from ranked
    window w_grp as (partition by year(sale_date) order by first_known_revenue_int, id rows between unbounded preceding and current row), w_grp_2 as (order by units_rank, id rows between 6 preceding and 1 preceding)
)
select
    id,
    store_id,
    product_id,
    units,
    units_rank,
    first_known_revenue_int,
    first_known_units,
    first_known_revenue_int_avg_6_prior,
    first_known_revenue_int_avg_3_prior
from enriched
order by units_rank;
