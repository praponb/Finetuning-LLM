with filtered_stores as (
    select *
    from stores
    where region in ('South', 'East', 'Central') and city in ('Chiang Mai', 'Berlin', 'Bangkok')
),
Agg as (
    select date_trunc('MONTH', opened_date) as opened_month, case when sqft >= 13722 then 'high' when sqft >= 7991 then 'mid' else 'low' end as sqft_bucket, sum(case when region = 'South' then 1 else 0 end) as south_count
    from filtered_stores
    group by date_trunc('MONTH', opened_date), case when sqft >= 13722 then 'high' when sqft >= 7991 then 'mid' else 'low' end
)
select opened_month, sqft_bucket, south_count
from Agg
where south_count < 78
order by opened_month, sqft_bucket;
