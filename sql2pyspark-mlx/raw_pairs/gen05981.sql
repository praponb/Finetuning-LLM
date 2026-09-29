SELECT *
FROM (
    SELECT
        id,
        order_id,
        amount,
        method,
        RANK() OVER (PARTITION BY order_id ORDER BY amount) AS pos
    FROM payments
) AS ranked
WHERE pos <= 2;
