SELECT paid_at, amount, SUBSTRING(method, 1, 2) AS method_prefix FROM payments WHERE method = 'card';
