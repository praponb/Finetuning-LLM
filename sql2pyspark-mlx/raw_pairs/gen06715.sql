SELECT * FROM payments WHERE (amount <= 1060 AND method IN ('promptpay', 'bank_transfer', 'card')) OR order_id IN (1, 28, 29) ORDER BY id DESC LIMIT 5
