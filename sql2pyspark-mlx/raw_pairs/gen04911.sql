WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC NULLS LAST, id ASC) AS rn
    FROM inventory
    QUALIFY rn = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(quantity) OVER (PARTITION BY product_id, store_id ORDER BY last_restock ASC, id ASC) AS next_quantity,
        LAG(quantity) OVER (PARTITION BY product_id, store_id ORDER BY last_restock ASC, id ASC) AS prev_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock ASC, id ASC) AS seq_no
    FROM latest
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        rn,
        prev_quantity,
        LEAD(next_quantity) OVER (PARTITION BY product_id ORDER BY last_restock ASC, id ASC) AS next_next_quantity,
        LEAD(prev_quantity) OVER (PARTITION BY product_id ORDER BY last_restock ASC, id ASC) AS next_prev_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock ASC, id ASC) AS row_seq,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY prev_quantity ASC, id ASC) AS prev_quantity_bucket
    FROM with_windows
    WHERE rn >= 1
),
latest_inventory AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY row_seq DESC NULLS LAST, id ASC) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
)
SELECT
    id,
    store_id,
    product_id,
    prev_quantity,
    next_prev_quantity,
    row_seq
FROM latest_inventory
