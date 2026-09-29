select
    case when city in ('Singapore', 'Bangkok') then 'tier_1' else 'other' end as city_group,
    case when rating >= 3.7 then 'high' when rating >= 2.3 then 'mid' else 'low' end as rating_bucket,
    avg(rating) as avg_rating,
    sort_array(collect_set(country)) as country_list
from suppliers
group by 1, 2
having avg(rating) >= 3.7;
