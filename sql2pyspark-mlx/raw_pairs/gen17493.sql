SELECT id, status, amount, order_id, RANK() OVER (PARTITION BY status ORDER BY amount DESC) AS rnk, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS amount_row FROM payments;
