SELECT id, product_id, quantity, PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS quantity_pct_rank FROM inventory;
