select ss.id, ss.revenue, ss.sale_date from store_sales ss where ss.store_id in (1, 2, 4, 6) and ss.product_id in (select id from products where category = 'Home')
