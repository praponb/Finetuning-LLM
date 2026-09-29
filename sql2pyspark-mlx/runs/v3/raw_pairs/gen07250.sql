WITH RankedOrders AS (
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY shipping_fee DESC) AS row_num
    FROM orders
)
SELECT
    id,
    status,
    shipping_fee,
    order_date
FROM RankedOrders
WHERE row_num = 1
ORDER BY order_date, 2 DESC
