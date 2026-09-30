SELECT * FROM (SELECT id, order_id, quantity, RANK() OVER (PARTITION BY order_id ORDER BY quantity) AS rnk FROM order_items) AS ranked WHERE rnk <= 3 ORDER BY rnk
