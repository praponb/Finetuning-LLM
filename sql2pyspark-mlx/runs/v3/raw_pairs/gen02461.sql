select
    case when s.city in ('Bangkok', 'New York') then 'priority' else 'other' end as city_group,
    s.city,
    max(p.amount) as max_amount,
    avg(p.amount) as avg_amount
from payments p
inner join orders o on o.id = p.order_id
left join stores s on o.store_id = s.id
group by case when s.city in ('Bangkok', 'New York') then 'priority' else 'other' end, s.city
order by max_amount, s.city desc, city_group
limit 10
