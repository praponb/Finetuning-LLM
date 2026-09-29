SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS pos FROM inventory WHERE quantity > 219) AS r WHERE pos <= 2
