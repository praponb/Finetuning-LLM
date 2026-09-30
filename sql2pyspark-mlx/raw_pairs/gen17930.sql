WITH with_windows AS (
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id) AS product_id_avg_quantity,
        LAG(quantity, 1) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id) AS prev_quantity,
        SUM(quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
    FROM inventory
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.product_id_avg_quantity,
    s.running_quantity,
    (SELECT MIN(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS min_inventory_quantity
FROM with_windows s
ORDER BY running_quantity;
