select ss.id, ss.revenue, ss.units, ss.product_id from store_sales ss where ss.product_id in (select id from products where launch_date between '2024-11-01' and '2025-03-01')
