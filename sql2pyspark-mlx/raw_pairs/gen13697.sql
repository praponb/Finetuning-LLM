SELECT status FROM payments UNION ALL SELECT status FROM shipments WHERE carrier IN ('Kerry', 'DHL');
