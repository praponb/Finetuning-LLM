WITH kept_keys AS (
    SELECT id
    FROM sessions
    INTERSECT ALL
    SELECT id
    FROM sessions
    WHERE customer_id IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM sessions s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    id,
    customer_id,
    device,
    started_at,
    CASE WHEN device RLIKE '^(mobile|tablet)$' THEN 'CORE' ELSE 'GROUP_B' END AS device_zone
FROM verified
ORDER BY customer_id
