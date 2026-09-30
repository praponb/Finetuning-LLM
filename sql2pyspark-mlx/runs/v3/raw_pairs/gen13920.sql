select
    s.city,
    s.region,
    count(o.discount) as discount_count,
    concat_ws(', ', sort_array(collect_set(o.status))) as status_values
from orders o
left outer join customers c on o.customer_id = c.id
join stores s on o.store_id = s.id
where s.sqft <= 15690
group by s.city, s.region
order by status_values desc
