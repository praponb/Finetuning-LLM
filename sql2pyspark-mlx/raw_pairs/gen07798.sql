with metrics as (
    select
        id,
        supplier_name,
        country,
        city,
        ntile(4) over (order by rating desc, id) as rating_bucket,
        sum(rating) over (partition by city order by rating, id rows between unbounded preceding and current row) as running_rating,
        sum(rating) over (partition by city order by rating, id rows between unbounded preceding and current row) as running_rating_2,
        count(rating) over (partition by city) as city_count_rating,
        last_value(city, true) over (partition by city order by rating, id rows between unbounded preceding and current row) as last_known_city
    from suppliers
)
select
    country,
    max(running_rating_2) as max_running_rating_2,
    count(*) as num_rows
from metrics
group by country
order by num_rows desc nulls first;
