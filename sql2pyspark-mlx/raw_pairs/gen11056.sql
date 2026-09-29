select ss.id, ss.product_id, ss.store_id
from store_sales ss
where month(ss.sale_date) in (3, 4, 12) and ss.store_id in (select id from stores where store_name like '%Cent%');
