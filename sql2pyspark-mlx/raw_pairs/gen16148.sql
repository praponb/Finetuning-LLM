SELECT id, product_id, last_restock
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS pos
    FROM inventory
) AS t
WHERE pos <= 2
