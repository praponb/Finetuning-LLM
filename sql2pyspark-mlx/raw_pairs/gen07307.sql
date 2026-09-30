select ss.id, ss.product_id, ss.revenue from store_sales ss where ss.product_id not in (select id from products where stock_qty = 301);
