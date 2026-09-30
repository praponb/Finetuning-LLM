select product_id, round(stddev(revenue), 2) as stddev_revenue from store_sales group by product_id;
