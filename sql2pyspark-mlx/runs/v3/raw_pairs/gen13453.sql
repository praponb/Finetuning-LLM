SELECT store_id, sale_date, units
FROM store_sales
WHERE MONTH(sale_date) IN (1, 6, 10) AND (revenue IS NULL OR units BETWEEN 18 AND 36)
