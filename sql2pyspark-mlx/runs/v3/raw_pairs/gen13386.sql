SELECT id, method FROM payments WHERE amount >= 750 UNION ALL SELECT id, method FROM payments WHERE amount BETWEEN 850 AND 1280;
