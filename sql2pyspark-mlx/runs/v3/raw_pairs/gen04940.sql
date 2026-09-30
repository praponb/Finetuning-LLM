with store_stats as (
    select store_id, count(*) as cnt
    from store_sales
    group by store_id
)
select s.id, s.city, ss.cnt
from stores s
inner join store_stats ss on s.id = ss.store_id
order by city;
