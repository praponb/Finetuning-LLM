select s.id, s.rating, s.city
from suppliers s
where exists (select 1 from products p where p.supplier_id = s.id and p.price between 650 and 690)
order by rating;
