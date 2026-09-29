select
    case when rating >= 4.0 then 'high' when rating >= 2.1 then 'mid' else 'low' end as rating_bucket,
    country,
    count(distinct rating) as unique_citys,
    count(distinct rating) as unique_countrys,
    min(rating) as min_rating
from suppliers
where rating <= 2.2
group by case when rating >= 4.0 then 'high' when rating >= 2.1 then 'mid' else 'low' end, country
order by country;
