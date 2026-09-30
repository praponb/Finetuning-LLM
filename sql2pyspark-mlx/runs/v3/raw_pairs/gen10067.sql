SELECT
    id,
    store_id,
    amount,
    customer_id
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY amount DESC) AS row_num
    FROM orders
    WHERE YEAR(order_date) >= 2025
) AS t
WHERE row_num <= 3;
