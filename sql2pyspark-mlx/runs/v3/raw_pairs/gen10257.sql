WITH Numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS pos FROM inventory) SELECT id, product_id, last_restock FROM Numbered WHERE pos <= 3
