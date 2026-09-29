with valid as (
    -- Keep recent, valid rows
    select
        id,
        store_name,
        city,
        opened_date,
        sqft,
        round(sqft * 0.85, 2) as sqft_scaled
    from stores
    where city != 'London'
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from valid s
    where exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.sale_date >= s.opened_date
        )
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        count(sqft_scaled) over (partition by city order by opened_date, id rows between unbounded preceding and current row) as running_count_sqft_scaled,
        max(sqft_scaled) over (partition by city order by opened_date, id rows between unbounded preceding and current row) as running_max_sqft_scaled,
        sum(sqft_scaled) over (partition by city order by opened_date, id rows between unbounded preceding and current row) as running_sum_sqft_scaled
    from eligible
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        store_name,
        opened_date,
        sqft_scaled,
        running_count_sqft_scaled,
        running_sum_sqft_scaled
    from (
        select *, row_number() over (partition by city order by running_max_sqft_scaled desc nulls last, id) as rank_in_group
        from metrics
    ) t
    where rank_in_group = 1
),
kept_keys as (
    -- Set intersection on keys
    select id
    from latest
    intersect
    select id
    from stores
    where region in ('North', 'West')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    inner join kept_keys k on s.id = k.id
)
-- Multi-level aggregation
select
    date_trunc('week', opened_date) as week_start,
    percentile_approx(sqft_scaled, array(0.25, 0.5, 0.75)) as sqft_scaled_quartiles,
    round(avg(sqft_scaled), 2) as avg_sqft_scaled
from reconciled
group by week_start
having avg_sqft_scaled < 1000000
order by week_start;
