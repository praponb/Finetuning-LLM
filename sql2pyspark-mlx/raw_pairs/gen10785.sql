select ss.id, ss.revenue, ss.sale_date, ss.store_id from store_sales ss where ss.product_id in (select id from products where launch_date between '2023-03-12' and '2024-08-01')
