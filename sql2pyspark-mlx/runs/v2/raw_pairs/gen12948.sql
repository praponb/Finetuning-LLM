select ss.id, ss.product_id, ss.units, ss.revenue from store_sales ss where ss.store_id in (select id from stores where sqft between 10271 and 13896);
