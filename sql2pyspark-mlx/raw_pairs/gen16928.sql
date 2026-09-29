SELECT method FROM payments WHERE status = 'FAILED' EXCEPT SELECT method FROM payments WHERE order_id IS NOT NULL;
