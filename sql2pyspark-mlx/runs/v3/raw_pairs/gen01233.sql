SELECT *
FROM (
    SELECT order_id, status, MAX(amount) AS max_amount
    FROM payments
    GROUP BY order_id, status
) AS t
WHERE max_amount >= 460
ORDER BY status;
