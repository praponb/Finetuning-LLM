select product_id, min_units, revenue_count from (select product_id, min(units) as min_units, count(revenue) as revenue_count from store_sales group by product_id) as x where min_units >= 30
