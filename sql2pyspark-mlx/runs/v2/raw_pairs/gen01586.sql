select ss.id, ss.product_id, ss.store_id, ss.sale_date from store_sales ss where ss.store_id not in (select id from stores where opened_date >= '2025-05-17');
