SELECT id, segment, IF(is_active, 'Yes', 'No') AS is_active_label
FROM customers
WHERE segment = 'Corporate'
ORDER BY is_active_label;
