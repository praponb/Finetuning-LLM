WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        ROUND(quantity * 0.85, 2) AS quantity_scaled
    FROM inventory
    WHERE last_restock > TO_DATE('2023-09-06')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(quantity_scaled, 0) AS quantity_scaled_rounded,
        quantity_scaled - quantity AS quantity_scaled_minus_quantity
    FROM base
    WHERE quantity = 86
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.quantity_scaled_rounded,
        b.category,
        b.brand,
        b.product_name
    FROM filtered s
    INNER JOIN products b
        ON s.product_id = b.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_scaled_rounded DESC NULLS FIRST, id) AS rn
        FROM enriched
    ) x
    WHERE rn = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(quantity_scaled_rounded, 1, 0) OVER (PARTITION BY brand ORDER BY quantity_scaled_rounded, id) AS prev_quantity_scaled_rounded,
        SUM(quantity_scaled_rounded) OVER (PARTITION BY product_id) AS product_id_sum_quantity_scaled_rounded,
        LEAD(quantity_scaled_rounded, 1) OVER (ORDER BY quantity_scaled_rounded, id) AS next_quantity_scaled_rounded
    FROM latest
    WHERE rn > 1
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        quantity_scaled_rounded,
        category,
        brand,
        prev_quantity_scaled_rounded,
        product_id_sum_quantity_scaled_rounded,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY next_quantity_scaled_rounded DESC, id) AS next_quantity_scaled_rounded_bucket,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY prev_quantity_scaled_rounded DESC, id) AS prev_quantity_scaled_rounded_bucket,
        NTILE(3) OVER (ORDER BY next_quantity_scaled_rounded DESC, id) AS next_quantity_scaled_rounded_bucket_2,
        RANK() OVER (ORDER BY prev_quantity_scaled_rounded DESC NULLS FIRST) AS prev_quantity_scaled_rounded_rank
    FROM scored
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        store_id,
        prev_quantity_scaled_rounded,
        quantity_scaled_rounded
    FROM enriched_inventory
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (prev_quantity_scaled_rounded AS PREV_QUANTITY_SCALED_ROUNDED, quantity_scaled_rounded AS QUANTITY_SCALED_ROUNDED)
) u;
