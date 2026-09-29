SELECT opened_date FROM accounts WHERE opened_date IS NOT NULL UNION ALL SELECT order_date AS opened_date FROM orders WHERE order_date IS NOT NULL;
