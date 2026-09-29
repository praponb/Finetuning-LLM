select
    s.id,
    s.country,
    t.first_launch_date,
    t.total_cost
from suppliers s
join (select supplier_id, min(launch_date) as first_launch_date, sum(cost) as total_cost
    from products
    group by supplier_id) t on s.id = t.supplier_id
where t.total_cost > 2700
order by total_cost desc
