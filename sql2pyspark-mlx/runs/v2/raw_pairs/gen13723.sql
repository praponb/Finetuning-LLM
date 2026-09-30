select
    s.rating,
    s.country,
    s.supplier_name,
    p.cost,
    p.stock_qty
from suppliers s
left outer join products p on p.supplier_id = s.id
where p.brand = 'Globex'
order by supplier_name;
