select ss.id, ss.revenue, ss.store_id, ss.sale_date from store_sales ss where ss.store_id not in (select id from stores where city != 'London') order by ss.store_id desc;
