SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS pos FROM inventory WHERE store_id IN (2, 4, 5, 6)) AS r WHERE pos <= 2
