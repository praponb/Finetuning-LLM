select
    case when sqft >= 14568 then 'high' when sqft >= 3166 then 'mid' else 'low' end sqft_bucket,
    sort_array(collect_set(region)) region_list,
    sum(sqft) total_sqft,
    avg(sqft) avg_sqft
from stores
group by sqft_bucket
having avg_sqft >= 9600
