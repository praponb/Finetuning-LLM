WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand = 'Acme'
        )
)
-- Aggregate with conditional metrics
SELECT
    country,
    CASE WHEN rating >= 3.6 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    ROUND(AVG(rating), 2) AS avg_rating,
    STDDEV_SAMP(rating) AS rating_stddev,
    ROUND(AVG(rating), 2) AS avg_rating_2,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS rating_per_london
FROM not_flagged
GROUP BY 1, 2;
