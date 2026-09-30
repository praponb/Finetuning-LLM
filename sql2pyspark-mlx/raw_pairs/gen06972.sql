SELECT id, sale_date, revenue, LEAD(revenue, 1) OVER (ORDER BY sale_date, id) AS next_revenue FROM store_sales WHERE sale_date BETWEEN '2024-06-01' AND '2024-11-25';
