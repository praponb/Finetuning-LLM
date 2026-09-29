with filtered_suppliers as (
    select *
    from suppliers
    where city not in ('Tokyo', 'New York') or rating < 2.5
),
suppliers_stats as (
    select city, max(rating) - min(rating) as rating_range, count(distinct rating) as unique_citys
    from filtered_suppliers
    group by city
)
select *
from suppliers_stats
order by rating_range desc, city asc
limit 3;
