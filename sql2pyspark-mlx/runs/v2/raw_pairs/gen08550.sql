select id, store_id, revenue, round(revenue / sum(revenue) over (partition by store_id), 3) as revenue_share from store_sales where store_id = 2
