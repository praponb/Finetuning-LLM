select s.country, s.supplier_name, p.brand
from suppliers s
left outer join products p on p.supplier_id = s.id
where p.category <> 'Books'
