select case when sqft >= 16542 then 'high' when sqft >= 4658 then 'mid' else 'low' end as sqft_bucket, city, min(opened_date) as min_opened_date
from stores
group by case when sqft >= 16542 then 'high' when sqft >= 4658 then 'mid' else 'low' end, city
order by city, sqft_bucket
limit 3;
