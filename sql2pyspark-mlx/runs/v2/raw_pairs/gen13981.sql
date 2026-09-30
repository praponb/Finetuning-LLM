select ss.id, ss.product_id, ss.store_id, ss.sale_date from store_sales ss where ss.product_id in (select id from products where category not in ('Electronics', 'Fashion'))
