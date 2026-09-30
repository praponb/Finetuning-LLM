select id, sale_date, store_id, revenue, percent_rank() over (partition by month(sale_date), store_id order by revenue desc) as revenue_pct_rank from store_sales order by revenue desc;
