SELECT *, CAST(is_active AS INT) AS is_active_flag
FROM customers
WHERE name IS NOT NULL AND name IS NOT NULL
