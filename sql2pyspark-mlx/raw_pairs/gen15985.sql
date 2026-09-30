with cleaned as (
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        lower(country) as country_lower
    from suppliers
    where city is distinct from 'Singapore'
      and supplier_name like 'Si%'
),
eligible as (
    select s.*
    from cleaned s
    where exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.cost < 550
        )
),
enriched as (
    select
        *,
        sum(rating) over (partition by city, country order by rating, id rows between unbounded preceding and current row) as running_sum_rating,
        min(rating) over (partition by city, country order by rating, id rows between unbounded preceding and current row) as running_min_rating,
        avg(rating) over (partition by city, country order by rating, id rows between unbounded preceding and current row) as running_avg_rating,
        max(rating) over (partition by city, country order by rating, id rows between unbounded preceding and current row) as running_max_rating,
        ntile(5) over (partition by city order by rating, id) as rating_bucket
    from eligible
),
valid as (
    select
        id,
        supplier_name,
        city,
        rating,
        country_lower,
        running_min_rating,
        running_max_rating,
        rating_bucket,
        lower(city) as city_lower
    from enriched
    where city is not null
      and rating_bucket != 5
)
select *
from (
    select
        id,
        city,
        rating,
        running_max_rating,
        running_min_rating
    from valid
) src
unpivot include nulls (
    val for measure in (rating, running_max_rating, running_min_rating)
);
