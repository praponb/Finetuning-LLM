WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC NULLS LAST, id) AS row_num
    FROM inventory
    QUALIFY row_num = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT store_id
    FROM latest
    EXCEPT ALL
    SELECT store_id
    FROM inventory
    WHERE product_id IN (4, 6, 10)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.store_id = k.store_id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_part quantity_dense_rank,
        MIN(quantity) OVER (PARTITION BY store_id, product_id) store_id_min_quantity
    FROM kept_rows
    WINDOW w_part AS (PARTITION BY store_id, product_id ORDER BY quantity)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        row_num,
        quantity_dense_rank,
        store_id_min_quantity,
        PERCENT_RANK() OVER (ORDER BY quantity DESC NULLS LAST) quantity_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity_dense_rank DESC NULLS LAST) quantity_dense_rank_percent_rank,
        RANK() OVER (ORDER BY store_id_min_quantity DESC NULLS FIRST) store_id_min_quantity_rank,
        CUME_DIST() OVER (ORDER BY store_id_min_quantity NULLS FIRST) store_id_min_quantity_cume_dist,
        RANK() OVER (ORDER BY quantity_dense_rank DESC) quantity_dense_rank_rank
    FROM enriched
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        row_num,
        quantity_dense_rank,
        quantity_percent_rank,
        store_id_min_quantity_rank,
        store_id_min_quantity_cume_dist,
        quantity_dense_rank_rank,
        ROUND(quantity * 0.85, 2) quantity_adjusted
    FROM scored
    WHERE last_restock > '2025-05-01'
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        d.category,
        d.launch_date,
        d.stock_qty
    FROM filtered src
    LEFT JOIN products d
        ON src.product_id = d.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        row_num,
        quantity_dense_rank,
        quantity_percent_rank,
        store_id_min_quantity_rank,
        store_id_min_quantity_cume_dist,
        quantity_dense_rank_rank,
        quantity_adjusted,
        category,
        launch_date,
        stock_qty,
        AVG(quantity) OVER (PARTITION BY product_id, store_id) product_id_avg_quantity,
        ROUND(store_id_min_quantity_rank / SUM(store_id_min_quantity_rank) OVER (PARTITION BY product_id), 4) store_id_min_quantity_rank_share
    FROM with_dims
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        store_id,
        product_id,
        category,
        store_id_min_quantity_rank_share
    FROM with_windows
) src
PIVOT (
    AVG(store_id_min_quantity_rank_share) AS amount,
    COUNT(1) AS n
    FOR category IN ('Sports', 'Books')
)
