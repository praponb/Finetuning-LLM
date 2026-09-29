select product_id, avg(revenue) as avg_revenue from store_sales where units != 31 group by 1;
