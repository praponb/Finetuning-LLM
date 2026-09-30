WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(quantity, 2) AS quantity_rounded
    FROM inventory
    WHERE product_id IN (4, 6, 8)
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(last_restock, 12) AS last_restock_plus_months
    FROM base
    WHERE last_restock > DATE '2023-03-02'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock_plus_months ASC, id ASC) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        quantity_rounded,
        rank_in_group,
        LEAD(quantity_rounded) OVER (PARTITION BY store_id ORDER BY rank_in_group ASC, id ASC) AS next_quantity_rounded,
        LAG(quantity_rounded) OVER (PARTITION BY store_id ORDER BY rank_in_group ASC, id ASC) AS prev_quantity_rounded,
        LAG(quantity) OVER (PARTITION BY store_id ORDER BY rank_in_group ASC, id ASC) AS prev_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY rank_in_group ASC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY rank_in_group ASC, id ASC) AS seq_no
    FROM deduped
    WHERE rank_in_group <= 1
),
base_inventory AS (
    -- Keep recent, valid rows
    SELECT
        *,
        seq_no - row_seq AS seq_no_minus_row_seq,
        next_quantity_rounded * prev_quantity AS next_quantity_rounded_times_prev_quantity
    FROM scored
    WHERE rank_in_group > 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY prev_quantity_rounded DESC NULLS LAST) AS dedup_rank
        FROM base_inventory
    ) t
    WHERE dedup_rank = 1
),
valid_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        quantity_rounded,
        rank_in_group,
        next_quantity_rounded,
        prev_quantity_rounded,
        row_seq,
        seq_no,
        seq_no_minus_row_seq,
        next_quantity_rounded_times_prev_quantity,
        dedup_rank,
        next_quantity_rounded + seq_no AS next_quantity_rounded_plus_seq_no,
        CAST(rank_in_group AS DOUBLE) AS rank_in_group_str
    FROM ranked
    WHERE prev_quantity_rounded >= 52
)
SELECT
    s.id,
    s.store_id,
    s.quantity_rounded,
    s.rank_in_group,
    s.next_quantity_rounded,
    s.row_seq,
    s.seq_no,
    s.seq_no_minus_row_seq,
    s.next_quantity_rounded_times_prev_quantity,
    (SELECT SUM(t2.units) FROM store_sales t2 WHERE t2.store_id = s.store_id) AS sum_store_sale_units
FROM valid_inventory s
ORDER BY seq_no DESC NULLS FIRST;
