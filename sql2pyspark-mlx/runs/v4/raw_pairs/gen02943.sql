SELECT id, status, shipping_fee
FROM (
    SELECT
        id,
        status,
        shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee DESC, id) AS rnk
    FROM orders
) AS ranked
WHERE rnk <= 3
ORDER BY status
