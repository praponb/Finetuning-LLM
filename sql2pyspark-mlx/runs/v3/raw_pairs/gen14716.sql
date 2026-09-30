select case when rating >= 4.4 then 'high' when rating >= 2.4 then 'mid' else 'low' end as rating_bucket, min(rating) as min_rating
from suppliers
where rating > 3.6 and (city = 'Singapore' or city in ('Bangkok', 'Singapore', 'New York'))
group by rating_bucket
order by min_rating
