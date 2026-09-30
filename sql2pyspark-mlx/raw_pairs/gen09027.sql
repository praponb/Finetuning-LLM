select
    case when rating >= 3.5 then 'high' when rating >= 1.9 then 'mid' else 'low' end as rating_bucket,
    country,
    max(rating) as max_rating,
    round(stddev(rating), 2) as stddev_rating
from suppliers
group by 1, 2
order by rating_bucket asc, country asc
limit 5
