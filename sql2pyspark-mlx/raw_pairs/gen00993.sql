select
    case when rating >= 4.1 then 'high' when rating >= 2.1 then 'mid' else 'low' end as rating_bucket,
    case when city in ('New York', 'Bangkok') then 'tier_1' else 'other' end as city_group,
    count(distinct rating) as unique_citys,
    round(stddev(rating), 2) as stddev_rating
from suppliers
group by case when rating >= 4.1 then 'high' when rating >= 2.1 then 'mid' else 'low' end, case when city in ('New York', 'Bangkok') then 'tier_1' else 'other' end
