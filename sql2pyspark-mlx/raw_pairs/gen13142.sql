select product_id, round(stddev(revenue), 2) as stddev_revenue, sum(units) as total_units from store_sales group by product_id order by total_units desc;
