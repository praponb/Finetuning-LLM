WITH Src AS (
    SELECT *
    FROM orders
    WHERE discount >= 0.12 OR customer_id = 32
),
agg AS (
    SELECT
        CASE WHEN shipping_fee >= 40 THEN 'high' WHEN shipping_fee >= 14 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket,
        status,
        AVG(amount) AS avg_amount,
        SUM(CASE WHEN status = 'PENDING' THEN shipping_fee ELSE 0 END) AS pending_shipping_fee
    FROM Src
    GROUP BY CASE WHEN shipping_fee >= 40 THEN 'high' WHEN shipping_fee >= 14 THEN 'mid' ELSE 'low' END, status
)
SELECT shipping_fee_bucket, status, pending_shipping_fee
FROM agg
ORDER BY pending_shipping_fee, status DESC, shipping_fee_bucket
LIMIT 5;
