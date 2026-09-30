select id, product_id, revenue, rank() over (partition by product_id order by revenue desc) as rnk, avg(revenue) over (partition by product_id) as product_id_avg_revenue from store_sales;
