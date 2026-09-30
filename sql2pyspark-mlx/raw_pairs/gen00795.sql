select ss.id, ss.units, ss.product_id, ss.sale_date from store_sales ss where ss.product_id not in (select id from products where brand in ('Umbrella', 'Acme', 'Initech'))
