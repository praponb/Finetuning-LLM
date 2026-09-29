select
    case when rating >= 3.6 then 'high' when rating >= 1.8 then 'mid' else 'low' end as rating_bucket,
    country,
    avg(rating) as avg_rating,
    sort_array(collect_set(country)) as country_list
from suppliers
where rating > 3.6 or supplier_name not like 'Si%'
group by case when rating >= 3.6 then 'high' when rating >= 1.8 then 'mid' else 'low' end, country
order by country desc, rating_bucket
limit 5
