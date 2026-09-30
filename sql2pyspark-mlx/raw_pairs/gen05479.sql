SELECT opened_date AS event_date FROM accounts WHERE customer_id IN (11, 14, 28) EXCEPT SELECT order_date AS event_date FROM orders;
