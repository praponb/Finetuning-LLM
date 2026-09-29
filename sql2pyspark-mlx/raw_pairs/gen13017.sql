SELECT *
FROM (
    SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity, COUNT(*) AS cnt
    FROM inventory
    WHERE quantity > 222
    GROUP BY product_id
) AS s
WHERE cnt < 7
ORDER BY cnt DESC;
