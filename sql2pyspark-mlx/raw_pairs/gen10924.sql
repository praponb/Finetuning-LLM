WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(quantity) quantity_whole
    FROM inventory
    WHERE quantity = 223
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity_whole,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY DATE_TRUNC('week', last_restock), product_id ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_quantity,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', last_restock), product_id ORDER BY last_restock DESC, id) row_seq,
        MIN(quantity_whole) OVER (PARTITION BY product_id) product_id_min_quantity_whole
    FROM deduped
),
metrics_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(first_known_quantity, 2) OVER (PARTITION BY product_id, store_id ORDER BY first_known_quantity, id) prev_first_known_quantity,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY row_seq DESC, id) row_seq_bucket,
        LEAD(first_known_quantity, 1) OVER (PARTITION BY product_id ORDER BY row_seq, id) next_first_known_quantity
    FROM metrics
    WHERE product_id IN (2, 4, 8, 10)
)
-- Ranking and gap features
SELECT
    id,
    store_id,
    quantity_whole,
    first_known_quantity,
    prev_first_known_quantity,
    ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity_whole, id) event_seq,
    SUM(next_first_known_quantity) OVER (ORDER BY first_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_next_first_known_quantity,
    AVG(row_seq) OVER (PARTITION BY product_id ORDER BY row_seq, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) row_seq_avg_6_prior,
    SUM(row_seq) OVER (PARTITION BY store_id ORDER BY next_first_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_row_seq
FROM metrics_inventory;
