select s.id, s.rating
from suppliers s
where s.id in (select supplier_id from products where upper(trim(product_name)) = 'YOGA MAT')
order by s.id desc, s.rating
limit 5
