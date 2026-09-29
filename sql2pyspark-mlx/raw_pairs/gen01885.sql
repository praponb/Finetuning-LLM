SELECT amount value FROM transactions WHERE txn_time >= '2025-01-01' EXCEPT SELECT price value FROM products WHERE category != 'Home' ORDER BY value;
