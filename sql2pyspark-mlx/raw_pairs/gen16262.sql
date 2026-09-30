select i.id, i.product_id
from inventory i
where i.store_id not in (select id from stores where upper(store_name) like '%ALL%')
order by id asc;
