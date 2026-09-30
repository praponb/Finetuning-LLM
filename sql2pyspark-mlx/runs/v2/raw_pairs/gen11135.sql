WITH numbered AS (SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY last_restock) AS rn FROM inventory) SELECT * FROM numbered WHERE rn <= 2 ORDER BY last_restock, quantity DESC;
