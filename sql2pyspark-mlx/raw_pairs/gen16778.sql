with base as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(store_name, 'unassigned') as store_name_filled,
        coalesce(region, 'UNKNOWN') as region_filled
    from stores
    where region in ('West', 'South')
)
-- Running and moving aggregates
select
    id,
    city,
    region,
    store_name_filled,
    sum(sqft) over w_hist as running_sum_sqft,
    max(sqft) over w_hist as running_max_sqft,
    min(sqft) over w_hist as running_min_sqft,
    avg(sqft) over w_hist as running_avg_sqft
from base
where city in ('London', 'Singapore', 'Chiang Mai')
window w_hist as (partition by region, city order by opened_date, id rows between unbounded preceding and current row)
