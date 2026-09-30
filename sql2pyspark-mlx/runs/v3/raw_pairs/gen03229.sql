SELECT id, store_id, LAST_DAY(last_restock) AS last_restock_month_end FROM inventory WHERE quantity BETWEEN 75 AND 167 AND product_id IN (2, 3, 4, 10) AND store_id IN (4, 5, 6) ORDER BY id;
