select
    s.city,
    p.cost,
    p.supplier_id,
    p.brand
from suppliers s
left join products p on p.supplier_id = s.id
where s.country in ('JP', 'SG', 'UK')
