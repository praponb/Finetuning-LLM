select
    s.region,
    case when s.city in ('Berlin', 'New York') then 'group_a' else 'other' end as city_group,
    count(distinct i.quantity) as unique_stores,
    count(*) as cnt
from inventory i
left join stores s on s.id = i.store_id
group by s.region, case when s.city in ('Berlin', 'New York') then 'group_a' else 'other' end
order by s.region asc, city_group asc
