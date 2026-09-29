WITH valid_payments AS (
    SELECT *, TO_DATE(paid_at) paid_at_date
    FROM payments
    WHERE method IN ('promptpay', 'card', 'paypal') AND amount >= 600
),
summary AS (
    SELECT method, CASE WHEN amount >= 1260 THEN 'high' WHEN amount >= 380 THEN 'mid' ELSE 'low' END amount_bucket, MAX(amount) max_amount
    FROM valid_payments
    GROUP BY method, CASE WHEN amount >= 1260 THEN 'high' WHEN amount >= 380 THEN 'mid' ELSE 'low' END
)
SELECT method, amount_bucket, max_amount
FROM summary
WHERE max_amount < 840;
