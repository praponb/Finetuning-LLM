select product_id, sum(units) as total_units, dense_rank() over (order by sum(units) desc) as units_rank from store_sales group by product_id;
