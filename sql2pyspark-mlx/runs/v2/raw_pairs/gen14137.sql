select id, store_id, sale_date, revenue, lag(revenue, 2, 0) over (partition by store_id order by sale_date, id) as prev_revenue from store_sales where units <= 34;
