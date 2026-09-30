SELECT id, sale_date, store_id
FROM store_sales
WHERE sale_date = (SELECT MAX(sale_date) FROM store_sales WHERE revenue >= 1300)
ORDER BY id, sale_date DESC
LIMIT 20
