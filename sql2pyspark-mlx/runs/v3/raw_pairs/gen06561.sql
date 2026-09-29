SELECT
    id,
    amount,
    discount,
    LAST_VALUE(discount) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_discount
FROM orders
WHERE store_id IS NOT NULL;
