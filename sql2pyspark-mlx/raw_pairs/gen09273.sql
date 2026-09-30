SELECT id, order_id, product_id, unit_price FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity DESC, id) AS dedup_rank FROM order_items) t WHERE dedup_rank = 1;
