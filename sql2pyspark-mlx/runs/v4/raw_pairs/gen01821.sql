SELECT cost AS value FROM shipments EXCEPT SELECT amount AS value FROM payments WHERE order_id IS NULL;
