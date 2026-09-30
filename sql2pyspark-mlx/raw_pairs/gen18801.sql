SELECT id, order_id, quantity FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rnk FROM order_items) AS r WHERE rnk <= 3
