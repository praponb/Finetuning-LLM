SELECT id, revenue, sale_date, LAST_DAY(sale_date) AS sale_date_month_end FROM store_sales WHERE sale_date BETWEEN '2023-12-27' AND '2024-01-01' ORDER BY sale_date_month_end, id LIMIT 20
