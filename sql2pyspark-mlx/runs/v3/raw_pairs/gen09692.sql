select *
from (
    select city, count(sqft) as sqft_count
    from stores
    where region = 'North'
    group by city
) as agg
where sqft_count < 5
order by sqft_count, city
limit 20;
