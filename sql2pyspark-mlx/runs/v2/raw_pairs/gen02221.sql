SELECT email, name, CAST(is_active AS INT) AS is_active_flag
FROM customers
WHERE (age BETWEEN 41 AND 61 AND country IN ('DE', 'UK')) OR age > 54
ORDER BY is_active_flag;
