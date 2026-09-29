WITH RankedOrders AS (
    SELECT
        id,
        customer_id,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) pos
    FROM orders
    WHERE discount <= 0.27
)
SELECT *
FROM RankedOrders
WHERE pos <= 2
