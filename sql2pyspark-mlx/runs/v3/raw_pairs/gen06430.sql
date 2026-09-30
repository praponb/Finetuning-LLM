select i.id, i.product_id
from inventory i
where month(i.last_restock) in (2, 3, 10) and i.store_id in (select id from stores where city in ('New York', 'Bangkok', 'Berlin'))
order by i.id desc
