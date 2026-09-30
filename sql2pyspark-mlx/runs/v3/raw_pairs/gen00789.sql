with store_stats as (
    select store_id, count(distinct discount) as unique_channels
    from orders
    group by store_id
)
select s.id, s.sqft, ss.unique_channels
from stores s
left join store_stats ss on s.id = ss.store_id
where ss.unique_channels >= 8
