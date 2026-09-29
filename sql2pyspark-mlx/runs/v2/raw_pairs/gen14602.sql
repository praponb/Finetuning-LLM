SELECT id, segment, is_active, IF(is_active, 'Yes', 'No') AS is_active_label FROM customers WHERE name IS NOT NULL ORDER BY id, segment LIMIT 50
