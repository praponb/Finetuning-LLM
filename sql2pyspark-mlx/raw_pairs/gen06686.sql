WITH store_summary AS (
    SELECT store_id, COUNT(DISTINCT revenue) unique_stores
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.opened_date,
    s.region,
    ss.unique_stores
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.unique_stores > 9
ORDER BY s.opened_date, ss.unique_stores DESC;
