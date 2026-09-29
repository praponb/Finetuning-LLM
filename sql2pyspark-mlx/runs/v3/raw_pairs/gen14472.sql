SELECT status FROM orders WHERE amount >= 1420 UNION ALL SELECT status FROM orders WHERE amount > 600 ORDER BY status
