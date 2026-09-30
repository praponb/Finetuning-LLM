select ss.id, ss.units from store_sales ss where ss.product_id in (select id from products where supplier_id is null) order by units desc, id desc;
