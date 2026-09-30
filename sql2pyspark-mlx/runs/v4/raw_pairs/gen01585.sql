select s.id, s.rating, s.country
from suppliers s
where s.country not in ('UK', 'SG', 'JP') and not exists (select 1 from products p where p.supplier_id = s.id and p.category <> 'Fashion')
