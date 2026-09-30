SELECT status FROM payments WHERE method = 'bank_transfer' UNION SELECT status FROM shipments;
