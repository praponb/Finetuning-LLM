SELECT * FROM (SELECT order_id, status, amount FROM payments WHERE method NOT IN ('paypal', 'promptpay', 'bank_transfer'))
PIVOT (MAX(amount) FOR status IN ('REFUNDED', 'FAILED'))
ORDER BY order_id;
