WITH deduped AS (SELECT id, order_id FROM order_items QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) = 1) SELECT id FROM deduped ORDER BY id
