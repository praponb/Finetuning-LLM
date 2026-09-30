SELECT id, order_id, amount, AVG(amount) OVER (PARTITION BY order_id) AS order_id_avg_amount FROM payments WHERE method IN ('bank_transfer', 'paypal');
