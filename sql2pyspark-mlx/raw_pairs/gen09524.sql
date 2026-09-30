SELECT order_date AS event_date FROM orders WHERE order_date IS NOT NULL UNION SELECT opened_date AS event_date FROM accounts WHERE opened_date IS NOT NULL ORDER BY event_date
