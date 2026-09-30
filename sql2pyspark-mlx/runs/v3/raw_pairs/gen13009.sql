SELECT
    id,
    product_id,
    sale_date,
    store_id,
    COALESCE(revenue, 0) AS revenue_filled
FROM store_sales
WHERE sale_date > '2025-03-01' AND revenue <= 3000
ORDER BY store_id, id
LIMIT 1
