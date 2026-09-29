select
    s.id,
    s.supplier_name,
    s.rating,
    t.avg_price
from suppliers s
join (select supplier_id, avg(price) as avg_price
    from products
    group by supplier_id) t on s.id = t.supplier_id
order by avg_price desc, s.supplier_name;
