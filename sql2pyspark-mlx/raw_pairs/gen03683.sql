WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS code
)
SELECT
    code,
    customer_id,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    MAX(created_at) AS last_created_at,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    SORT_ARRAY(COLLECT_SET(code)) AS code_list,
    COUNT(*) FILTER (WHERE code = 'SPRING10') AS spring10_rows,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE code = 'VIP') AS vip_rows,
    GROUPING(code) AS grouping_level
FROM flattened
GROUP BY GROUPING SETS ((code, customer_id, DATE_TRUNC('DAY', created_at)), (code, customer_id), (code));
