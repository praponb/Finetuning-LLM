select p.id, p.category
from products p
where p.supplier_id in (select id from suppliers where length(supplier_name) < 9)
order by category;
