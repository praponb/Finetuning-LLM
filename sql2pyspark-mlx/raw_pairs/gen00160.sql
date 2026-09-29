select s.id, s.city
from suppliers s
where s.id in (select supplier_id from products)
