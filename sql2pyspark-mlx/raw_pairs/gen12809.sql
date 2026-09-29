WITH flattened AS (
    SELECT id, value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS value
)
SELECT
    value,
    COUNT(*) FILTER (WHERE value = 'SPRING10') AS spring10_rows,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE value = 'WELCOME5') AS welcome5_rows
FROM flattened
GROUP BY value;
