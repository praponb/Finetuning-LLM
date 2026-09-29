WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rn FROM order_items) SELECT id, order_id, quantity FROM ranked WHERE rn <= 1 ORDER BY id;
