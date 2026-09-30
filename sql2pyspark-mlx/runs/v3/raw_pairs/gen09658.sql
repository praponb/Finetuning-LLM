select p.id, p.category, p.brand, p.stock_qty from products p where not exists (select 1 from store_sales ss where ss.product_id = p.id and ss.store_id in (4, 5));
