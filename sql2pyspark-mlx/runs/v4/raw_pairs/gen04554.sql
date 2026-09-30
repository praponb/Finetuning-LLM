select
    s.country,
    s.rating,
    p.cost,
    p.supplier_id
from suppliers s
left outer join products p on s.id = p.supplier_id
where s.country = 'UK';
