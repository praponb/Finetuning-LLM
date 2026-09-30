SELECT
    id,
    discount,
    order_date,
    amount
FROM orders
WHERE discount >= (SELECT MAX(discount) FROM orders WHERE discount > 0.35);
