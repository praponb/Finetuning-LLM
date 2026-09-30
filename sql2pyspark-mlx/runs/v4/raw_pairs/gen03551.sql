SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    LEAD(quantity, 2, 0) OVER w_grp AS next_quantity,
    LAG(quantity, 2) OVER (ORDER BY quantity ASC, id ASC) AS prev_quantity,
    NTILE(5) OVER w_grp AS quantity_bucket
FROM inventory
WINDOW w_grp AS (PARTITION BY product_id ORDER BY quantity ASC, id ASC);
