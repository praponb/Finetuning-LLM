SELECT amount FROM payments WHERE method NOT IN ('promptpay', 'bank_transfer') UNION SELECT price FROM products
