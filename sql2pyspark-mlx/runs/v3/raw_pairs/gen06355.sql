select ss.id, ss.product_id, ss.store_id, ss.revenue from store_sales ss where ss.product_id in (select id from products where stock_qty between 151 and 166) order by ss.id
