select s.id, s.sqft
from stores s
where exists (select 1 from inventory i where i.store_id = s.id and year(i.last_restock) = 2025)
order by s.sqft desc;
