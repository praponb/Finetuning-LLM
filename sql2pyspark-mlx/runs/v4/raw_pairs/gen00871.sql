WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(quantity) OVER w_ordered AS store_id_stddev_samp_quantity,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        DENSE_RANK() OVER w_recent AS quantity_dense_rank,
        LEAD(quantity, 2) OVER w_running AS next_quantity
    FROM inventory
    WINDOW w_ordered AS (PARTITION BY store_id), w_recent AS (ORDER BY quantity), w_running AS (PARTITION BY store_id ORDER BY last_restock, id)
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    store_id_stddev_samp_quantity,
    quantity_dense_rank,
    next_quantity
FROM with_windows
ORDER BY last_restock DESC NULLS LAST;
