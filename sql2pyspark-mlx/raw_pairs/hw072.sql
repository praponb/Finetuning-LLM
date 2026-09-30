SELECT id, CONCAT_WS('-', country, city, segment) AS customer_key FROM customers;
