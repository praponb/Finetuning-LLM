SELECT id, customer_id, discount
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY discount) AS pos
    FROM orders
) AS ranked
WHERE pos = 1;
