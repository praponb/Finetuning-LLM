select p.id, p.supplier_id, p.launch_date from products p where p.supplier_id is null and not exists (select 1 from store_sales ss where ss.product_id = p.id);
