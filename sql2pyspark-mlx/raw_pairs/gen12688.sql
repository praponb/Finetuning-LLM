WITH joined AS (
    SELECT e.*, o.order_date
    FROM payments e
    LEFT JOIN orders o
        ON e.order_id = o.id
)
SELECT
    method,
    CASE WHEN amount >= 1510 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    MAX(paid_at) AS ended_at,
    COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
    MAX(paid_at) AS ended_at_2,
    SIZE(COLLECT_SET(method)) AS num_methods,
    COUNT(*) FILTER (WHERE method = 'paypal') AS paypal_rows,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list
FROM joined
GROUP BY method, CASE WHEN amount >= 1510 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY amount_tier DESC NULLS LAST
