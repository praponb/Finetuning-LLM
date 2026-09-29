select
    case when rating >= 4.5 then 'high' when rating >= 1.8 then 'mid' else 'low' end as rating_bucket,
    country,
    sum(case when country = 'JP' then rating else 0 end) as jp_rating,
    max(rating) - min(rating) as rating_range,
    max(rating) as max_rating
from suppliers
group by case when rating >= 4.5 then 'high' when rating >= 1.8 then 'mid' else 'low' end, country;
