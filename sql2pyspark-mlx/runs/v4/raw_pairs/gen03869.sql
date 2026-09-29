SELECT store_id, product_id, total_count
FROM (
    SELECT store_id, product_id, COUNT(*) AS total_count
    FROM inventory
    GROUP BY store_id, product_id
) AS t
WHERE total_count < 6;
