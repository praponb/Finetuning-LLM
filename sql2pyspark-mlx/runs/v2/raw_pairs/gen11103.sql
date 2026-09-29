SELECT id, name, IF(is_active, 'Yes', 'No') AS is_active_label FROM customers WHERE segment = 'SMB' ORDER BY 1 DESC
