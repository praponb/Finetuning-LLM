select ss.id, ss.sale_date from store_sales ss where ss.store_id in (select id from stores where month(opened_date) in (6, 9, 12)) order by ss.id desc limit 5;
