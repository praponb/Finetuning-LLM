WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        ADD_MONTHS(last_restock, 12) AS last_restock_plus_months
    FROM inventory
    WHERE last_restock > DATE '2023-03-24'
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.quantity,
        a.last_restock,
        p.city,
        p.store_name
    FROM filtered a
    LEFT JOIN stores p
        ON a.store_id = p.id
    WHERE p.city IN ('New York', 'Bangkok', 'London')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        LEAD(quantity, 1) OVER (PARTITION BY product_id ORDER BY quantity, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY quantity, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id) AS event_seq,
        COUNT(quantity) OVER (PARTITION BY store_id) AS store_id_count_quantity
    FROM with_dims
),
scored_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY event_seq DESC, id) AS seq_no_2,
        LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY seq_no, id) AS prev_quantity
    FROM scored
)
-- Ranking and gap features
SELECT
    id,
    quantity,
    seq_no_2,
    prev_quantity,
    LAG(prev_quantity) OVER w_running AS prev_prev_quantity,
    LAG(seq_no_2) OVER w_running AS prev_seq_no_2,
    ROW_NUMBER() OVER w_running AS event_seq_2,
    LAST_VALUE(quantity) IGNORE NULLS OVER w_hist AS last_known_quantity
FROM scored_inventory
WINDOW w_running AS (PARTITION BY product_id ORDER BY event_seq, id), w_hist AS (PARTITION BY YEAR(last_restock), product_id ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
