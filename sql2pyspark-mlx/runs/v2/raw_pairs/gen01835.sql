SELECT *, LENGTH(method) method_length, CONCAT(method, '/', status) method_status
FROM payments
WHERE (amount < 1520 AND method NOT IN ('paypal', 'card')) OR method IS NOT NULL;
