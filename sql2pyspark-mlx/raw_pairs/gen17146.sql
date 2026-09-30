with src as (
    select *
    from store_sales
    where year(sale_date) = 2024
),
RevenueBucketSummary as (
    select case when revenue >= 3200 then 'high' when revenue >= 1400 then 'mid' else 'low' end as revenue_bucket, max(units) - min(units) as units_range, count(*) as cnt
    from src
    group by case when revenue >= 3200 then 'high' when revenue >= 1400 then 'mid' else 'low' end
)
select revenue_bucket, units_range, cnt
from RevenueBucketSummary
where cnt < 4
order by 2 asc
