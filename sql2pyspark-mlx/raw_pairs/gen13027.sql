with base as (
    select
        *,
        lower(country) as country_lower,
        case when country in ('US', 'JP') then 'Priority' else 'Other' end as country_group,
        coalesce(city, 'UNKNOWN') as resolved_city
    from suppliers
    where city is not null
      and country = 'TH'
)
select
    id,
    supplier_name,
    country,
    city,
    country_lower,
    country_group,
    resolved_city,
    avg(rating) over (partition by country order by rating asc, id asc rows between 5 preceding and current row) as rating_avg_5,
    last_value(city, true) over (partition by city, country order by rating asc, id asc rows between unbounded preceding and current row) as last_known_city
from base
