SELECT cost FROM products WHERE product_name LIKE 'Ba%' EXCEPT SELECT amount FROM transactions WHERE txn_time >= '2025-03-01'
