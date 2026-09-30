WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM web_events
    QUALIFY RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    CASE WHEN duration_sec >= 369 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN page = '/cart' THEN 1 END), 0), 2) AS duration_sec_per_cart,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', event_time) ORDER BY ROUND(AVG(duration_sec), 2) DESC) AS avg_duration_sec_rank
FROM deduped
GROUP BY DATE_TRUNC('YEAR', event_time), DATE_TRUNC('MONTH', event_time), CASE WHEN duration_sec >= 369 THEN 'HIGH' ELSE 'LOW' END
ORDER BY search_rows
