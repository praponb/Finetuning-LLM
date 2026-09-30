SELECT *
FROM (
    SELECT
        id,
        order_id,
        amount,
        status,
        RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS pos
    FROM payments
) AS ranked
WHERE pos <= 1
