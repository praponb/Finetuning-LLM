WITH top_orders AS (
    SELECT
        id,
        status,
        shipping_fee,
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee DESC, id) AS rnk
    FROM orders
)
SELECT *
FROM top_orders
WHERE rnk <= 3
ORDER BY status ASC
