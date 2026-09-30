SELECT status FROM payments WHERE paid_at >= '2023-05-01' EXCEPT SELECT status FROM shipments WHERE cost BETWEEN 32 AND 87 ORDER BY status;
