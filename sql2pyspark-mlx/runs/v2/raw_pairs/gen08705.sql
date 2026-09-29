WITH store_summary AS (
    SELECT store_id, AVG(units) AS avg_units, COUNT(*) AS num_store_sales
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    s.sqft,
    ss.avg_units,
    ss.num_store_sales
FROM stores s
LEFT OUTER JOIN store_summary ss ON s.id = ss.store_id
ORDER BY sqft DESC
