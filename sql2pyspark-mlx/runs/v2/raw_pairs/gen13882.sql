SELECT
    id,
    status,
    discount,
    channel
FROM (
    SELECT
        id,
        status,
        discount,
        channel,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount DESC, id) AS pos
    FROM orders
    WHERE order_date BETWEEN '2024-10-01' AND '2025-04-04'
) AS ranked
WHERE pos <= 1
