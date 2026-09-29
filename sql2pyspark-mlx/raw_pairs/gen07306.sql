WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS element
)
SELECT
    element,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    MIN(created_at) AS first_seen,
    MAX(created_at) AS last_seen,
    MAX(created_at) AS ended_at,
    COUNT(*) FILTER (WHERE element = 'SPRING10') AS spring10_rows,
    RANK() OVER (PARTITION BY element ORDER BY COUNT(*) FILTER (WHERE element = 'SPRING10') DESC) AS spring10_rows_rank
FROM flattened
GROUP BY 1, 2;
