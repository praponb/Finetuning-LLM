select product_id, count(*) as cnt from store_sales group by product_id order by product_id desc;
