WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS rn FROM inventory) SELECT * FROM ranked WHERE rn = 1 ORDER BY last_restock DESC, store_id;
