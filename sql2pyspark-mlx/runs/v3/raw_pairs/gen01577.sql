SELECT product_id, QUARTER(last_restock) last_restock_quarter, store_id, MAX(quantity) max_quantity FROM inventory GROUP BY product_id, QUARTER(last_restock), store_id
