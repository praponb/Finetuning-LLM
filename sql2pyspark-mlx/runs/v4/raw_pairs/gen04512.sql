select ss.id, ss.units, ss.sale_date, ss.product_id from store_sales ss where ss.product_id in (select id from products where product_name is not null)
