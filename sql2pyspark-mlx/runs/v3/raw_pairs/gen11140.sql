select s.id, s.supplier_name, s.country
from suppliers s
where s.rating is not null and s.id in (select supplier_id from products where cost between 340 and 450);
