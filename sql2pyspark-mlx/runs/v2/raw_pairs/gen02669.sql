WITH RankedPayments AS (
    SELECT
        id,
        order_id,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS pos
    FROM payments
    WHERE amount < 1380
)
SELECT *
FROM RankedPayments
WHERE pos = 1;
