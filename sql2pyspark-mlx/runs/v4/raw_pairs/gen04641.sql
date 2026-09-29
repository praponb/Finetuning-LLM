select *
from (
    select city, sum(case when city = 'Bangkok' then rating else 0 end) as bangkok_rating, sort_array(collect_set(city)) as city_list
    from suppliers
    where supplier_name like 'Tok%'
    group by city
) as agg
where bangkok_rating >= 59;
