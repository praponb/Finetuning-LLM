SELECT
    id,
    credit_limit,
    country,
    IF(is_active, 'Yes', 'No') AS is_active_label
FROM customers
WHERE city IN ('Singapore', 'Tokyo', 'New York');
