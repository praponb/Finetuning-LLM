WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS rnk FROM inventory) SELECT * FROM numbered WHERE rnk <= 2
