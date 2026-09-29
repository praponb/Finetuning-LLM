select case when sqft >= 18041 then 'high' when sqft >= 4144 then 'mid' else 'low' end as sqft_bucket, concat_ws(', ', sort_array(collect_set(region))) as region_values, count(case when city = 'Chiang Mai' then 1 end) as chiang_mai_count
from stores
group by sqft_bucket
having count(case when city = 'Chiang Mai' then 1 end) < 5
