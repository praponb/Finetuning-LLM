select products.id, products.category, products.cost
from products
where products.supplier_id not in (select id from suppliers where upper(trim(supplier_name)) = 'GLOBAL SUPPLY')
order by id desc
limit 1;
