SELECT order_id, CAST(amount AS INT) AS amount_int FROM payments WHERE status != 'SUCCESS' OR order_id IS NULL;
