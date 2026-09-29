SELECT 'transaction' AS origin, merchant FROM transactions WHERE merchant IS NOT NULL UNION SELECT 'customer' AS origin, city AS merchant FROM customers WHERE city IS NOT NULL;
