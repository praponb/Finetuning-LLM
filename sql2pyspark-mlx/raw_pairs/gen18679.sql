WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.quantity BETWEEN 5 AND 6
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN shipping_fee >= 42 THEN 'LARGE' ELSE 'SMALL' END AS shipping_fee_tier,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    SUM(CASE WHEN channel = 'partner' THEN shipping_fee ELSE 0 END) AS partner_shipping_fee,
    ROUND(AVG(amount), 2) AS avg_amount,
    MIN(discount) AS min_discount,
    COUNT(DISTINCT id) AS distinct_ids
FROM kept
GROUP BY GROUPING SETS ((CASE WHEN shipping_fee >= 42 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', order_date)), (CASE WHEN shipping_fee >= 42 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY distinct_ids;
