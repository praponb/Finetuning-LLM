WITH Ranked AS (SELECT id, store_id, quantity, product_id, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS rnk FROM inventory) SELECT * FROM Ranked WHERE rnk <= 3 ORDER BY rnk
