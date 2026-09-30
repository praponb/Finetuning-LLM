WITH flattened AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, attr_value
)
SELECT
    k,
    COUNT(CASE WHEN k = 'device' THEN 1 END) AS device_count,
    COUNT(CASE WHEN k = 'gift' THEN 1 END) AS gift_count
FROM flattened
GROUP BY k
ORDER BY k DESC NULLS FIRST
