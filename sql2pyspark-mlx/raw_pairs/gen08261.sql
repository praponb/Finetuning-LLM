select id, sale_date, product_id, revenue, round(revenue / sum(revenue) over (partition by date_trunc('MONTH', sale_date), product_id), 3) as revenue_share from store_sales;
