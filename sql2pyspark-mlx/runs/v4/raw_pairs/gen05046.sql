WITH ranked_orders AS (
    SELECT
        id,
        customer_id,
        shipping_fee,
        amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS rnk
    FROM orders
)
SELECT *
FROM ranked_orders
WHERE rnk <= 2
