select case when sqft >= 16016 then 'high' when sqft >= 7368 then 'mid' else 'low' end as sqft_bucket, count(distinct sqft) as unique_regions, round((100 * sum(case when region = 'East' then 1 else 0 end)) / count(*), 1) as pct_east
from stores
group by case when sqft >= 16016 then 'high' when sqft >= 7368 then 'mid' else 'low' end
having count(distinct sqft) >= 3
order by unique_regions asc, sqft_bucket desc;
