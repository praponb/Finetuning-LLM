SELECT id, product_id, quantity FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS FIRST, id) AS rn FROM order_items) t WHERE rn = 1
