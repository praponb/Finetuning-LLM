SELECT id, revenue, DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
FROM store_sales
WHERE sale_date <= '2025-07-20' AND (units < 23 OR units < 15)
