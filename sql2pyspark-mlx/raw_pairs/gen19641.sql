WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        ADD_MONTHS(last_restock, 6) AS last_restock_plus_months
    FROM inventory
    WHERE last_restock > DATE '2023-04-14'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rn
        FROM cleaned
    ) t
    WHERE rn = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        quantity,
        rn
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_plus_months DESC, id) AS dedup_rank
        FROM ranked
    ) x
    WHERE dedup_rank = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        quantity - rn AS quantity_minus_rn,
        quantity - rn AS quantity_minus_rn_2
    FROM latest
    WHERE quantity IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN quantity >= 235 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(quantity_minus_rn), 2) AS avg_quantity_minus_rn,
    COUNT(*) AS num_rows,
    ROUND(AVG(quantity_minus_rn), 2) AS avg_quantity_minus_rn_2,
    BIT_AND(quantity) AS quantity_bit_and,
    STDDEV_SAMP(quantity) AS quantity_stddev
FROM valid
GROUP BY CASE WHEN quantity >= 235 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_quantity_minus_rn_2
