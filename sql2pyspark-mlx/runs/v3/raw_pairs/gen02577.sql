select country, case when rating >= 4.2 then 'high' when rating >= 2.3 then 'mid' else 'low' end as rating_bucket, min(rating) as min_rating
from suppliers
where rating > 3.5
group by country, rating_bucket
having min(rating) > 2.8
order by rating_bucket, country
limit 10
