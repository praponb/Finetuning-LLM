WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS rnk FROM inventory) SELECT * FROM ranked WHERE rnk <= 2 ORDER BY rnk DESC, store_id DESC;
