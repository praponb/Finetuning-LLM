select s.id, s.city, s.supplier_name
from suppliers s
where s.city in ('Bangkok', 'New York', 'Tokyo') and exists (select 1 from products p where p.supplier_id = s.id);
