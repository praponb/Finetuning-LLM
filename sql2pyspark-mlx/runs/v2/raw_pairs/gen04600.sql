select id, store_id, revenue, percent_rank() over (partition by store_id order by revenue desc) revenue_pct_rank from store_sales where store_id in (1, 4) order by id asc;
