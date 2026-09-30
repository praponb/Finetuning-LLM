WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS pos FROM inventory) SELECT * FROM numbered WHERE pos <= 5 ORDER BY last_restock;
