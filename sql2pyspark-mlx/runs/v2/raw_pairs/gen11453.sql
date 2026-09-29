SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY amount DESC) AS rn
    FROM orders
) AS ranked
WHERE rn <= 2;
