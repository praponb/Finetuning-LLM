select ss.id, ss.product_id, ss.revenue, ss.units from store_sales ss where ss.product_id in (select id from products where supplier_id in (16, 20)) order by units desc, id limit 50;
