WITH latest_match AS (
    SELECT
        s.id,
        s.name,
        s.city,
        s.credit_limit,
        latest.status,
        latest.order_discount
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.status, c.discount AS order_discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    CASE WHEN credit_limit >= 12200 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT_IF(status = 'SHIPPED') AS shipped_events,
    AVG(order_discount) AS avg_order_discount
FROM latest_match
GROUP BY CASE WHEN credit_limit >= 12200 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_order_discount DESC NULLS LAST;
