SELECT id, order_id, quantity FROM (SELECT id, order_id, quantity, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rnk FROM order_items) AS t WHERE rnk <= 5 ORDER BY id DESC;
