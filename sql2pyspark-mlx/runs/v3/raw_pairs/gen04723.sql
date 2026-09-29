select p.id, p.product_name from products p where exists (select 1 from store_sales ss where ss.product_id = p.id) order by product_name;
