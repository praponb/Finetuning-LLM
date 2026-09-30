WITH top_orders AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS pos
    FROM orders
)
SELECT
    id,
    customer_id,
    shipping_fee,
    order_date
FROM top_orders
WHERE pos <= 3;
