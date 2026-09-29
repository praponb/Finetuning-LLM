SELECT id, store_id
FROM store_sales
WHERE sale_date >= '2025-05-08' AND sale_date > '2025-05-01' AND YEAR(sale_date) >= 2023
ORDER BY id DESC, store_id DESC
