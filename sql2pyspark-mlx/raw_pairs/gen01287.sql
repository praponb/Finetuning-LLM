SELECT *
FROM (
    SELECT product_id, store_id, COUNT(*) AS cnt
    FROM inventory
    GROUP BY product_id, store_id
) AS sub
WHERE cnt > 4
