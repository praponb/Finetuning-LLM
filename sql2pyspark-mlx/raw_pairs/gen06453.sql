select s.id, s.supplier_name
from suppliers s
where not exists (select 1 from products p where p.supplier_id = s.id and p.category not in ('Electronics', 'Sports', 'Home'))
