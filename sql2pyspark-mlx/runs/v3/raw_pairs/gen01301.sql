SELECT id, amount, order_id, ROUND(amount, 2) amount_rounded FROM payments WHERE amount BETWEEN 560 AND 1360 AND (status IN ('REFUNDED', 'FAILED', 'SUCCESS') OR method != 'cash')
