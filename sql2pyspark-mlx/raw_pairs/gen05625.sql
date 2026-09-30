WITH exploded AS (
    SELECT
        id,
        created_at,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, attr_value
)
SELECT
    key,
    COUNT(DISTINCT key) AS distinct_keys,
    COUNT(*) FILTER (WHERE key = 'gift') AS gift_rows,
    MAX(created_at) AS ended_at,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min
FROM exploded
GROUP BY key
ORDER BY duration_min DESC;
