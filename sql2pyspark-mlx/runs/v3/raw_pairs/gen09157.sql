SELECT
    id,
    discount,
    amount,
    status,
    CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END AS is_cancelled
FROM orders
WHERE NOT (amount < 1430 OR amount > 440)
