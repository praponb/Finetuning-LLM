SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY discount DESC) AS rnk
    FROM orders
    WHERE status = 'SHIPPED'
) AS t
WHERE rnk = 1;
