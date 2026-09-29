with Base as (
    select *
    from suppliers
    where country in ('TH', 'UK', 'JP')
),
agg as (
    select
        case when rating >= 3.6 then 'high' when rating >= 2.3 then 'mid' else 'low' end rating_bucket,
        country,
        round(stddev(rating), 2) stddev_rating,
        sum(case when country = 'TH' then rating else 0 end) th_rating
    from Base
    group by case when rating >= 3.6 then 'high' when rating >= 2.3 then 'mid' else 'low' end, country
)
select
    rating_bucket,
    country,
    stddev_rating,
    th_rating
from agg
where stddev_rating >= 59
order by stddev_rating;
