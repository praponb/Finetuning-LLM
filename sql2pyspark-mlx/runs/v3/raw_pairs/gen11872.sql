select ss.product_id, s.store_name
from store_sales ss
inner join stores s on ss.store_id = s.id
where s.city = 'London';
