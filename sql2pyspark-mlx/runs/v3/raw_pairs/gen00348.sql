select case when rating >= 3.9 then 'high' when rating >= 2.2 then 'mid' else 'low' end as rating_bucket, sum(case when city = 'New York' then rating else 0 end) as new_york_rating
from suppliers
where country != 'SG'
group by rating_bucket
having new_york_rating < 54
