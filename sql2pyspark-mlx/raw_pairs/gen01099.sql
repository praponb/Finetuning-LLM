WITH exploded AS (
    SELECT
        id,
        created_at,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, attr_value
)
SELECT
    k,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(CASE WHEN k = 'gift' THEN 1 END) AS gift_count,
    COUNT(DISTINCT k) AS distinct_ks,
    MAX(created_at) AS last_created_at,
    COUNT_IF(k = 'gift') AS gift_events
FROM exploded
GROUP BY 1, 2
HAVING COUNT_IF(k = 'gift') >= 1
ORDER BY gift_count DESC NULLS FIRST;
