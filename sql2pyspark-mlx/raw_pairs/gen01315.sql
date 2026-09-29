SELECT *, CAST(is_active AS INT) AS is_active_flag, COALESCE(name, 'Unknown') AS name_filled
FROM customers
WHERE segment <> 'Consumer' AND email LIKE '%@outlook.com'
ORDER BY is_active_flag DESC;
