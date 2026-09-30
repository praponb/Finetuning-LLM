WITH StoreTotals AS (
    SELECT store_id, COUNT(DISTINCT revenue) AS unique_products, COUNT(DISTINCT revenue) AS unique_stores
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.store_name,
    s.opened_date,
    S.unique_products,
    S.unique_stores
FROM stores s
LEFT JOIN StoreTotals S ON s.id = S.store_id
WHERE S.unique_products >= 4
ORDER BY S.unique_stores DESC
