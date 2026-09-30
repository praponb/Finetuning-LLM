SELECT id, order_id, quantity, RANK() OVER (PARTITION BY order_id ORDER BY quantity) AS rnk FROM order_items WHERE unit_price BETWEEN 150 AND 310 ORDER BY id;
