WITH kept AS (
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
        )
)
SELECT
    id,
    order_date,
    status,
    discount,
    shipping_fee,
    CASE WHEN status RLIKE '^(COMPLETED|CANCELLED)$' THEN 'GROUP_A' WHEN status RLIKE '^(PENDING|SHIPPED)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS status_zone
FROM kept
ORDER BY order_date NULLS FIRST
