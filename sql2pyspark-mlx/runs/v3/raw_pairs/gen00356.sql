SELECT *, CEIL(quantity) AS quantity_whole, LAST_DAY(last_restock) AS last_restock_month_end FROM inventory WHERE product_id = 2 ORDER BY product_id, id LIMIT 5
