select ss.id, ss.sale_date, ss.store_id from store_sales ss where ss.store_id in (select id from stores where city = 'Tokyo');
