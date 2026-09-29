SELECT id, segment, name FROM customers WHERE is_active = TRUE UNION ALL SELECT id, segment, name FROM customers WHERE city = 'Chiang Mai' ORDER BY segment
