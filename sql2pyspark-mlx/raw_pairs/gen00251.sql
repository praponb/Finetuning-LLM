WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        ROUND(quantity, 1) AS quantity_rounded,
        unit_price - quantity AS line_total
    FROM order_items
    WHERE product_id IN (22, 24, 33)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        FIRST_VALUE(quantity_rounded) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_rounded,
        NTILE(5) OVER w_running AS quantity_rounded_bucket
    FROM cleaned
    WHERE quantity_rounded > 49
    WINDOW w_running AS (PARTITION BY product_id ORDER BY quantity_rounded DESC, id)
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity_rounded_bucket, 1) AS quantity_rounded_bucket_rounded
    FROM enriched
    WHERE product_id IS NOT NULL
),
cleaned_order_items AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN first_known_quantity_rounded >= 69 THEN 'High' WHEN first_known_quantity_rounded >= 27 THEN 'Medium' ELSE 'Low' END AS first_known_quantity_rounded_level
    FROM valid
    WHERE first_known_quantity_rounded BETWEEN 36 AND 79
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        quantity_rounded_bucket_rounded - quantity_rounded_bucket AS quantity_rounded_bucket_rounded_minus_quantity_rounded_bucket,
        ROUND(quantity_rounded_bucket * 0.9, 2) AS adjusted_quantity_rounded_bucket
    FROM cleaned_order_items
    WHERE quantity_rounded_bucket = 5
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_rounded_bucket NULLS LAST, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
)
-- Ranking and gap features
SELECT
    id,
    product_id,
    quantity_rounded_bucket,
    quantity_rounded_bucket_rounded,
    first_known_quantity_rounded_level,
    NTILE(5) OVER (PARTITION BY product_id ORDER BY first_known_quantity_rounded DESC, id) AS first_known_quantity_rounded_bucket,
    RANK() OVER (PARTITION BY product_id ORDER BY adjusted_quantity_rounded_bucket DESC) AS adjusted_quantity_rounded_bucket_rank,
    LAG(quantity_rounded_bucket, 2) OVER (ORDER BY adjusted_quantity_rounded_bucket, id) AS prev_quantity_rounded_bucket
FROM ranked
