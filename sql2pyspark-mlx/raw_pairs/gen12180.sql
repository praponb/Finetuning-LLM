with valid as (
    -- Filter the raw rows and derive helper columns
    select *, date_add(opened_date, 38) opened_date_plus_38d
    from stores
    where opened_date > date '2023-06-08'
      and region is not null
),
metrics as (
    -- Ranking and gap features
    select
        id,
        opened_date_plus_38d,
        max(sqft) over (partition by city, region order by opened_date asc, id asc rows between unbounded preceding and current row) running_max_sqft,
        sum(sqft) over (partition by city, region order by opened_date asc, id asc rows between unbounded preceding and current row) running_sum_sqft,
        avg(sqft) over (partition by city, region order by opened_date asc, id asc rows between unbounded preceding and current row) running_avg_sqft,
        avg(sqft) over (partition by region, city order by opened_date_plus_38d asc, id asc rows between 6 preceding and 1 preceding) sqft_avg_6_prior
    from valid
    where sqft = 7441
),
matched_keys as (
    -- Set difference on keys
    select id
    from metrics
    except all
    select id
    from stores
    where sqft < 5452
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join matched_keys k on s.id = k.id
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        opened_date_plus_38d,
        running_max_sqft,
        running_avg_sqft,
        sqft_avg_6_prior + running_sum_sqft sqft_avg_6_prior_plus_running_sum_sqft,
        date_sub(opened_date_plus_38d, 66) opened_date_plus_38d_minus_66d
    from verified
    where year(opened_date_plus_38d) = 2025
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.sale_date <= s.opened_date_plus_38d
        )
),
report as (
    select
        id,
        running_max_sqft,
        running_avg_sqft,
        opened_date_plus_38d_minus_66d
    from not_flagged
    order by running_max_sqft asc nulls first
)
select
    id,
    running_max_sqft,
    running_avg_sqft
from report
order by running_avg_sqft asc nulls last
