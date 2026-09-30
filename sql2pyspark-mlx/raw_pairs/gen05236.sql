SELECT *
FROM (
    SELECT order_id, COUNT(*) AS row_count
    FROM payments
    GROUP BY order_id
) AS sub
WHERE row_count >= 4;
