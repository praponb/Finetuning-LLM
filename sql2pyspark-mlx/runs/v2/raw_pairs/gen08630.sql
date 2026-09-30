select
    p.id,
    p.category,
    p.price,
    p.supplier_id
from products p
where p.category not in ('Books', 'Home', 'Fashion') and p.supplier_id not in (select id from suppliers where country = 'SG');
