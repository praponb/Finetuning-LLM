SELECT
    store_id,
    SUM(quantity) total_quantity,
    COUNT(*) cnt,
    DENSE_RANK() OVER (ORDER BY SUM(quantity) DESC) AS quantity_rank
FROM inventory
GROUP BY store_id
ORDER BY store_id, 3;
