with filtered_suppliers as (
    select *
    from suppliers
    where rating >= 2.4
),
CountrySummary as (
    select
        country,
        case when rating >= 4.5 then 'high' when rating >= 1.6 then 'mid' else 'low' end as rating_bucket,
        sort_array(collect_set(city)) as city_list,
        avg(rating) as avg_rating
    from filtered_suppliers
    group by country, case when rating >= 4.5 then 'high' when rating >= 1.6 then 'mid' else 'low' end
)
select *
from CountrySummary
where avg_rating < 2.8
