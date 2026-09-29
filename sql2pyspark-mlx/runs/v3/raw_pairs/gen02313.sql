select id, store_id
from inventory
where last_restock between '2024-10-24' and '2025-01-23'
except
select id, store_id
from inventory
where last_restock <= '2025-02-14'
order by id;
