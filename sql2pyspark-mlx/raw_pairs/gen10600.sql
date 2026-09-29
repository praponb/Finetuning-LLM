WITH lines AS (
    SELECT
        base.id,
        base.created_at,
        base.coupon_codes,
        line.qty,
        line.price,
        line.promo
    FROM carts base
    LATERAL VIEW OUTER EXPLODE(base.items) t AS line
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count
FROM lines
GROUP BY 1
ORDER BY row_count DESC NULLS LAST
