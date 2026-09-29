select
    s.id,
    s.rating,
    s.country,
    t.last_launch_date,
    t.electronics_count
from suppliers s
inner join (select supplier_id, max(launch_date) as last_launch_date, count(case when category = 'Electronics' then 1 end) as electronics_count
    from products
    where supplier_id = 18
    group by supplier_id) t on s.id = t.supplier_id
where t.electronics_count >= 5
