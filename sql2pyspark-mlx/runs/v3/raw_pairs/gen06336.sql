select
    city,
    country,
    avg_rating,
    tokyo_rating
from (
    select
        city,
        country,
        round(avg(rating), 2) avg_rating,
        sum(case when city = 'Tokyo' then rating else 0 end) tokyo_rating
    from suppliers
    group by city, country
) agg
where avg_rating >= 69
