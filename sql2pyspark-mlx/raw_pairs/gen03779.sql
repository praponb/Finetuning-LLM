SELECT id, order_id, amount, paid_at FROM (SELECT id, order_id, amount, paid_at, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS pos FROM payments) AS r WHERE pos <= 1;
