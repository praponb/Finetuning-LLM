select p.id, p.product_name
from products p
where p.cost <= 440 and p.supplier_id in (select id from suppliers where rating >= 3.0)
