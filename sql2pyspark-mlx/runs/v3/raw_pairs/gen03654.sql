select
    region,
    case when sqft >= 13010 then 'high' when sqft >= 7326 then 'mid' else 'low' end as sqft_bucket,
    month(opened_date) as opened_month,
    count(*) as row_count,
    count(*) as cnt
from stores
group by region, case when sqft >= 13010 then 'high' when sqft >= 7326 then 'mid' else 'low' end, month(opened_date);
