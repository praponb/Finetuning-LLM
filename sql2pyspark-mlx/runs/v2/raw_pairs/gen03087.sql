select
    s.id,
    s.city,
    s.supplier_name,
    t.avg_price
from suppliers s
join (select supplier_id, round(avg(price), 2) as avg_price
    from products
    group by supplier_id) t on s.id = t.supplier_id
order by avg_price asc
