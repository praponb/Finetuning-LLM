select id, store_id, revenue, dense_rank() over (partition by store_id order by revenue desc) as dense_rnk from store_sales where store_id in (2, 3, 4, 6) order by revenue desc;
