SELECT order_id, status, TRIM(status) AS clean_status
FROM payments
WHERE method <> 'paypal';
