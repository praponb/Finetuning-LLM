select product_id, sum(revenue) total_revenue, sum(sum(revenue)) over (order by product_id) as cumulative_revenue from store_sales where product_id in (2, 3, 4, 7) group by product_id
