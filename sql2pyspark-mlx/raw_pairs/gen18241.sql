WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        RANK() OVER w_ordered AS quantity_rank,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        LEAD(quantity, 1) OVER w_grp AS next_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        ROW_NUMBER() OVER w_ordered_2 AS row_seq
    FROM inventory
    WHERE quantity >= 60
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY quantity DESC), w_grp AS (PARTITION BY store_id ORDER BY last_restock, id), w_ordered_2 AS (PARTITION BY product_id ORDER BY last_restock, id)
)
SELECT
    s.id,
    s.product_id,
    s.running_quantity,
    s.next_quantity,
    s.cumulative_quantity,
    s.row_seq,
    (SELECT SUM(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS sum_inventory_quantity
FROM with_windows s
ORDER BY sum_inventory_quantity NULLS FIRST;
