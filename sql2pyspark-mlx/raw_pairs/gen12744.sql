SELECT id, method, order_id, amount, ROW_NUMBER() OVER (PARTITION BY method, order_id ORDER BY amount DESC, id) AS amount_row FROM payments;
