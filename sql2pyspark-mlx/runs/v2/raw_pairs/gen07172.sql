select p.id, p.category
from products p
where p.supplier_id in (select id from suppliers where city = 'New York');
