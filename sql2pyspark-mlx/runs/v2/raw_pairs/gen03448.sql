WITH store_summary AS (
    SELECT store_id, COUNT(DISTINCT revenue) unique_stores, MAX(units) max_units
    FROM store_sales
    WHERE store_id = 3
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    ss.unique_stores,
    ss.max_units
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.unique_stores >= 4;
