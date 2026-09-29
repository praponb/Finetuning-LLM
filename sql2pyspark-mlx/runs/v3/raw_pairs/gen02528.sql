SELECT *
FROM (
    SELECT
        id,
        channel,
        order_date,
        RANK() OVER (PARTITION BY channel ORDER BY order_date DESC) AS pos
    FROM orders
    WHERE customer_id = 1
) AS ranked
WHERE pos <= 1
