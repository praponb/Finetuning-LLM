SELECT id, store_id, last_restock, quantity FROM (SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY last_restock DESC) AS rnk FROM inventory) AS ranked WHERE rnk <= 3 ORDER BY id DESC
