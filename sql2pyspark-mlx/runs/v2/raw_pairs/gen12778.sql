SELECT status, order_id FROM payments WHERE method NOT IN ('bank_transfer', 'card')
