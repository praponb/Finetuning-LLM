select ss.id, ss.product_id, ss.revenue
from store_sales ss
where ss.product_id in (select id from products where launch_date between '2024-07-01' and '2025-07-01')
