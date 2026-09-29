SELECT store_id, COALESCE(revenue, 0) revenue_filled
FROM store_sales
WHERE sale_date BETWEEN '2024-08-01' AND '2025-04-10'
