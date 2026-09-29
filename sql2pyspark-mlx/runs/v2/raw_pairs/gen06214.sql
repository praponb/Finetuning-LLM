SELECT
    id,
    customer_id,
    shipping_fee,
    status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS rn
    FROM orders
) AS t
WHERE rn <= 5
ORDER BY customer_id
