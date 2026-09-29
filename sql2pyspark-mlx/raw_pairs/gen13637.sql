select case when rating >= 3.5 then 'high' when rating >= 2.4 then 'mid' else 'low' end as rating_bucket, sum(case when city = 'Bangkok' then rating else 0 end) as bangkok_rating
from suppliers
where city not in ('Tokyo', 'London', 'Berlin') and rating between 3.4 and 3.7 and length(supplier_name) < 6
group by 1
