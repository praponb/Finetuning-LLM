WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.unit_price <= 190
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    CASE WHEN discount >= 0.36 THEN 'HIGH' ELSE 'LOW' END AS discount_tier,
    CASE WHEN amount >= 1400 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    COUNT(*) AS row_count,
    SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END) AS completed_amount,
    COUNT_IF(channel = 'partner') AS partner_events,
    SUM(CASE WHEN status = 'RETURNED' THEN amount ELSE 0 END) AS returned_amount
FROM not_flagged
GROUP BY DATE_TRUNC('MONTH', order_date), CASE WHEN discount >= 0.36 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN amount >= 1400 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY activity_month DESC
