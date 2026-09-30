with FilteredStores as (
    select *
    from stores
    where opened_date <= '2024-04-07'
),
opened_month_summary as (
    select date_trunc('MONTH', opened_date) as opened_month, case when sqft >= 12724 then 'high' when sqft >= 4930 then 'mid' else 'low' end as sqft_bucket, sum(case when region = 'South' then 1 else 0 end) as south_count
    from FilteredStores
    group by date_trunc('MONTH', opened_date), case when sqft >= 12724 then 'high' when sqft >= 4930 then 'mid' else 'low' end
)
select *
from opened_month_summary
where south_count < 45
order by opened_month asc, south_count desc, sqft_bucket asc
limit 10
