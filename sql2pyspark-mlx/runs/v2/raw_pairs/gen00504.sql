SELECT *
FROM (
    SELECT status, COUNT(*) AS cnt
    FROM orders
    GROUP BY status
) AS s
WHERE cnt >= 4;
