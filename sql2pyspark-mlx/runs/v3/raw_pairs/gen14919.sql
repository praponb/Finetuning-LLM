select p.id, p.launch_date
from products p
where p.supplier_id in (select id from suppliers where country = 'DE');
