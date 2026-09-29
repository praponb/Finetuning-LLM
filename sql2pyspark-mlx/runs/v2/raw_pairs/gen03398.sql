SELECT id, customer_id, order_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS rn FROM orders) AS r WHERE rn <= 1 ORDER BY id, order_date DESC;
