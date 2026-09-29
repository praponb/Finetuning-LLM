SELECT status, SUM(CASE WHEN method = 'bank_transfer' THEN amount ELSE 0 END) AS bank_transfer_amount FROM payments WHERE method NOT IN ('card', 'cash') GROUP BY status ORDER BY status;
