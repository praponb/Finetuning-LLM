WITH top_orders AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date DESC, id) AS rnk
    FROM orders
    WHERE order_date < '2023-08-01'
)
SELECT
    id,
    status,
    order_date,
    shipping_fee
FROM top_orders
WHERE rnk <= 5;
