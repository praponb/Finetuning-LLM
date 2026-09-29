SELECT * FROM shipments WHERE cost BETWEEN 50 AND 92 AND (carrier = 'Flash' OR order_id IS NOT NULL);
