SELECT id, order_date, DATEDIFF(CURRENT_DATE(), order_date) / 365 AS years_since_order_date FROM orders ORDER BY order_date, id;
