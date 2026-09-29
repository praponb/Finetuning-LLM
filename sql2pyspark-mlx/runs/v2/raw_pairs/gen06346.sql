SELECT id, sale_date, store_id, revenue - units AS revenue_per_unit FROM store_sales WHERE sale_date < '2025-02-01' AND (MONTH(sale_date) IN (8, 9, 12) OR units < 36) ORDER BY store_id DESC
