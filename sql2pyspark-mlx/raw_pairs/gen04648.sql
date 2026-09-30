select case when sqft >= 14828 then 'high' when sqft >= 6755 then 'mid' else 'low' end sqft_bucket, quarter(opened_date) opened_quarter, concat_ws(', ', sort_array(collect_set(region))) region_values
from stores
group by sqft_bucket, opened_quarter
having sum(sqft) < 41000
order by sqft_bucket asc, region_values asc;
