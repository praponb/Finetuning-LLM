SELECT * FROM payments WHERE order_id IS NULL AND status = 'SUCCESS' ORDER BY amount ASC
