SELECT id, order_id, paid_at FROM (SELECT id, order_id, paid_at, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS pos FROM payments) AS r WHERE pos <= 3 ORDER BY order_id;
