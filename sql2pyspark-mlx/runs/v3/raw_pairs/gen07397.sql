select id, store_id, revenue, avg(revenue) over (partition by store_id) as store_id_avg_revenue from store_sales where store_id = 4 order by 2, revenue desc;
