with src as (
    select *, trim(country) as clean_country
    from suppliers
    where not (lower(trim(supplier_name)) = 'global supply' and supplier_name like 'Tok%')
),
agg as (
    select case when rating >= 4.0 then 'high' when rating >= 2.5 then 'mid' else 'low' end as rating_bucket, country, round(avg(rating), 2) as avg_rating
    from src
    group by case when rating >= 4.0 then 'high' when rating >= 2.5 then 'mid' else 'low' end, country
)
select rating_bucket, avg_rating
from agg
order by rating_bucket
