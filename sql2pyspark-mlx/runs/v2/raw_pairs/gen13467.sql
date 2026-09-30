select *
from (
    select case when sqft >= 15486 then 'high' when sqft >= 3264 then 'mid' else 'low' end sqft_bucket, max(sqft) - min(sqft) sqft_range, sum(sqft) total_sqft
    from stores
    where store_name like 'O%'
    group by case when sqft >= 15486 then 'high' when sqft >= 3264 then 'mid' else 'low' end
) x
where total_sqft > 96000
order by sqft_range desc, sqft_bucket
limit 5;
