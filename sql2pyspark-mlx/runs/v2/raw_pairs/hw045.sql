SELECT id, sale_date, revenue,
       AVG(revenue) OVER (ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7
FROM store_sales;
