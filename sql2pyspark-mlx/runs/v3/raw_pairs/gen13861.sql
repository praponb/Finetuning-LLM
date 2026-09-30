WITH base AS (
    SELECT *
    FROM payments
    WHERE paid_at >= '2025-05-01' AND amount IS NULL
),
summary AS (
    SELECT order_id, SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END) AS card_count, COUNT(amount) AS amount_count
    FROM base
    GROUP BY order_id
)
SELECT order_id, card_count
FROM summary
WHERE card_count < 25
ORDER BY card_count, order_id
LIMIT 20;
