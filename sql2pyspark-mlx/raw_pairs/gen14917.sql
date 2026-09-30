SELECT * FROM payments WHERE (amount < 520 AND status = 'SUCCESS') OR status = 'REFUNDED'
