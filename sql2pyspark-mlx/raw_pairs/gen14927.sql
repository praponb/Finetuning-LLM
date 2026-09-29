WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock >= s.opened_date
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    AVG(sqft) FILTER (WHERE region = 'South') AS south_avg_sqft,
    COUNT(*) FILTER (WHERE region = 'East') AS east_rows
FROM eligible
GROUP BY DATE_TRUNC('YEAR', opened_date)
HAVING east_rows > 1
ORDER BY year_start DESC NULLS LAST;
