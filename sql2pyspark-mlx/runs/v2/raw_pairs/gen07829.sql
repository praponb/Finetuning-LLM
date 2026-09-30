select product_id, store_id from store_sales where product_id = 2 union all select product_id, store_id from store_sales where product_id = 7 order by product_id desc;
