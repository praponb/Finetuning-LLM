WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), last_restock) / 365.25 AS years_since_last_restock,
        ROUND(quantity * 100, 2) AS adjusted_quantity
    FROM inventory
    WHERE last_restock >= DATE '2023-06-28'
)
-- Join a dimension and aggregate
SELECT
    s.region,
    s.city,
    COUNT(*) AS num_rows,
    ROUND(AVG(e.years_since_last_restock), 2) AS avg_years_since_last_restock,
    MAX(e.quantity) AS max_quantity
FROM cleaned e
LEFT JOIN stores s ON e.store_id = s.id
GROUP BY s.region, s.city
ORDER BY avg_years_since_last_restock DESC NULLS LAST
