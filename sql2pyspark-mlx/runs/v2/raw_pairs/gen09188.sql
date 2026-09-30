select s.id, s.store_name, s.region
from stores s
where s.city = 'London' and exists (select 1 from orders o where o.store_id = s.id and o.amount between 1490 and 1530);
