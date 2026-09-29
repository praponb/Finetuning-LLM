SELECT id, method, status, amount, UPPER(method) AS method_upper FROM payments WHERE method = 'card';
