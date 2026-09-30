select product_id, sum(revenue) as total_revenue, sum(sum(revenue)) over (order by product_id) as cumulative_revenue from store_sales group by product_id order by product_id desc;
