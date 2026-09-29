select
    case when rating >= 4.1 then 'high' when rating >= 2.3 then 'mid' else 'low' end as rating_bucket,
    count(*) as supplier_count,
    max(rating) - min(rating) as rating_range,
    sort_array(collect_set(city)) as city_list
from suppliers
where supplier_name like '%Global%' and supplier_name like '%Globa%'
group by case when rating >= 4.1 then 'high' when rating >= 2.3 then 'mid' else 'low' end
order by rating_bucket asc, supplier_count desc
limit 1
