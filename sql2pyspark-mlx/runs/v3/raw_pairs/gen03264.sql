SELECT store_id, max_quantity, total_count
FROM (
    SELECT store_id, MAX(quantity) max_quantity, COUNT(*) total_count
    FROM inventory
    GROUP BY store_id
) x
WHERE max_quantity < 240;
