SELECT amount, order_id, method, TRIM(method) AS clean_method FROM payments WHERE method = 'card' ORDER BY 4, method DESC;
