select id, sale_date, revenue, lead(revenue, 1) over (order by sale_date, id) as next_revenue from store_sales;
