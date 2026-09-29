SELECT
    id,
    status,
    shipping_fee,
    discount
FROM (
    SELECT
        id,
        status,
        shipping_fee,
        discount,
        RANK() OVER (PARTITION BY status ORDER BY shipping_fee DESC) AS pos
    FROM orders
) AS t
WHERE pos <= 1
