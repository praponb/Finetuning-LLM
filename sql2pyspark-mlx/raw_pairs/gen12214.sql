with filtered_stores as (
    select *, substring(store_name, 1, 4) as store_name_prefix
    from stores
    where city in ('New York', 'Tokyo')
),
region_summary as (
    select region, avg(sqft) as avg_sqft, count(sqft) as sqft_count
    from filtered_stores
    group by region
)
select *
from region_summary
where avg_sqft < 11000
order by avg_sqft desc;
