select
    case when rating >= 4.2 then 'high' when rating >= 2.0 then 'mid' else 'low' end as rating_bucket,
    country,
    count(case when city = 'London' then 1 end) as london_count,
    round(stddev(rating), 2) as stddev_rating
from suppliers
where country <> 'DE'
group by 1, 2;
