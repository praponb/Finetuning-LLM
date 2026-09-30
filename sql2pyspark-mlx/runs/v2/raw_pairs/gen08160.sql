SELECT
    id,
    customer_id,
    amount,
    shipping_fee
FROM (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk
    FROM orders
) AS ranked
WHERE rnk = 1;
