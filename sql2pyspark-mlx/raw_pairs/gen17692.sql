WITH kept_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM suppliers
    INTERSECT ALL
    SELECT id, city
    FROM suppliers
    WHERE country IN ('JP', 'UK', 'DE')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    JOIN kept_keys k ON s.id = k.id AND s.city = k.city
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN rating >= 4.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    city,
    PERCENTILE_APPROX(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    COUNT(*) FILTER (WHERE country = 'UK') AS uk_rows,
    STDDEV_SAMP(rating) AS rating_stddev,
    COUNT(*) AS event_count,
    COUNT(*) AS cnt,
    RANK() OVER (PARTITION BY CASE WHEN rating >= 4.3 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) FILTER (WHERE country = 'UK') DESC) AS uk_rows_rank
FROM kept_rows
GROUP BY 1, 2
HAVING STDDEV_SAMP(rating) < 1000000
ORDER BY rating_tier;
