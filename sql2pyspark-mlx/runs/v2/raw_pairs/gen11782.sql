select city, case when rating >= 4.6 then 'high' when rating >= 2.0 then 'mid' else 'low' end as rating_bucket, round(stddev(rating), 2) as stddev_rating
from suppliers
group by city, case when rating >= 4.6 then 'high' when rating >= 2.0 then 'mid' else 'low' end
having stddev_rating < 25
