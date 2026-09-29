SELECT id, store_id, quantity, RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS rnk FROM inventory WHERE quantity BETWEEN 210 AND 212 ORDER BY store_id;
