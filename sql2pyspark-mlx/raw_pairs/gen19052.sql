select product_id, count(units) as units_count from store_sales where product_id = 9 and revenue < 3100 group by product_id having units_count < 4;
