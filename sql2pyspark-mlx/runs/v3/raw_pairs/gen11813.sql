SELECT status, cancelled_amount
FROM (
    SELECT status, SUM(CASE WHEN status = 'CANCELLED' THEN amount ELSE 0 END) AS cancelled_amount
    FROM orders
    WHERE store_id IN (5, 26, 29, 37)
    GROUP BY status
) AS t
WHERE cancelled_amount < 54
ORDER BY status;
