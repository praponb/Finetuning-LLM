WITH matched_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM inventory
    EXCEPT
    SELECT product_id, id
    FROM inventory
    WHERE quantity BETWEEN 130 AND 161
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN matched_keys k ON s.product_id = k.product_id AND s.id = k.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
    COUNT(*) AS cnt,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    ROUND(AVG(quantity), 2) AS avg_quantity_2,
    APPROX_PERCENTILE(quantity, 0.5) AS quantity_p50
FROM kept_rows
GROUP BY DATE_TRUNC('YEAR', last_restock)
ORDER BY avg_quantity DESC NULLS LAST;
