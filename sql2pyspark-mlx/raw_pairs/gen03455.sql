with src as (
    select *
    from suppliers
    where lower(trim(supplier_name)) = 'pacific wholesale'
),
RatingBucketSummary as (
    select
        case when rating >= 4.2 then 'high' when rating >= 2.0 then 'mid' else 'low' end as rating_bucket,
        country,
        sort_array(collect_set(city)) as city_list,
        concat_ws(', ', sort_array(collect_set(country))) as country_values
    from src
    group by case when rating >= 4.2 then 'high' when rating >= 2.0 then 'mid' else 'low' end, country
)
select *
from RatingBucketSummary;
