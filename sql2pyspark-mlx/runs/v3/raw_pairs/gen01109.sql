SELECT
    id,
    status,
    order_date,
    channel
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY order_date DESC) rnk
    FROM orders
) t
WHERE rnk <= 3;
