SELECT
    id,
    discount,
    amount,
    customer_id
FROM orders
WHERE discount >= 0.18 AND discount >= 0.39 AND order_date <= '2025-02-01'
ORDER BY discount DESC, id
LIMIT 20
