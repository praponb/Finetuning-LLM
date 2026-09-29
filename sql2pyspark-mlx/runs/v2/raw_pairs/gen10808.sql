SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS pos
    FROM inventory
    WHERE product_id IN (4, 5)
) AS r
WHERE pos = 1
ORDER BY product_id DESC, store_id DESC;
