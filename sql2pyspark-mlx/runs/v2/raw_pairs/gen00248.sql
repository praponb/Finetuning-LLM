select s.id, s.country, s.city
from suppliers s
where exists (select 1 from products p where p.supplier_id = s.id and p.supplier_id in (17, 37));
