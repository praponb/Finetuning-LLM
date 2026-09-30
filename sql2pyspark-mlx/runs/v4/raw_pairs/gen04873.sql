SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS dedup_rank FROM order_items) x WHERE dedup_rank = 1;
