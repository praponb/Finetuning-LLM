WITH filtered_payments AS (
    SELECT *
    FROM payments
    WHERE paid_at >= '2024-05-01' AND amount BETWEEN 480 AND 1580
),
summary AS (
    SELECT
        order_id,
        status,
        SUM(CASE WHEN method = 'bank_transfer' THEN amount ELSE 0 END) bank_transfer_amount,
        ROUND(AVG(amount), 2) avg_amount
    FROM filtered_payments
    GROUP BY order_id, status
)
SELECT *
FROM summary
WHERE avg_amount >= 76
ORDER BY 3 ASC, order_id ASC, status ASC
LIMIT 10
