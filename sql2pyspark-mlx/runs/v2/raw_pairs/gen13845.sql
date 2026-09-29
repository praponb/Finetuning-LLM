with recent_stores as (
    select *
    from stores
    where opened_date <= '2025-05-01' or sqft <> 10936
),
Agg as (
    select case when sqft >= 16766 then 'high' when sqft >= 2611 then 'mid' else 'low' end as sqft_bucket, avg(sqft) as avg_sqft, sum(case when city = 'Tokyo' then 1 else 0 end) as tokyo_count
    from recent_stores
    group by case when sqft >= 16766 then 'high' when sqft >= 2611 then 'mid' else 'low' end
)
select sqft_bucket, avg_sqft
from Agg
where tokyo_count > 60;
