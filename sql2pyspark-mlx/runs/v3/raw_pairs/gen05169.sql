select i.id, i.product_id, i.last_restock
from inventory i
where i.store_id not in (select id from stores where store_name like 'R%')
order by last_restock desc, i.id desc
