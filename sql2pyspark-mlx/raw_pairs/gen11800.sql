WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        ROUND(quantity, 0) AS quantity_rounded,
        ROUND(quantity * 0.85, 2) AS adjusted_quantity
    FROM inventory
    WHERE last_restock >= DATE '2023-07-07'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_id,
        quantity_rounded,
        adjusted_quantity
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY adjusted_quantity DESC, id) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
),
filtered_inventory AS (
    -- Basic cleansing and defaults
    SELECT *, quantity_rounded - adjusted_quantity AS quantity_rounded_minus_adjusted_quantity
    FROM ranked
    WHERE product_id IN (3, 6, 8)
),
filtered_inventory_2 AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(quantity_rounded, 0) AS quantity_rounded_filled,
        quantity_rounded - quantity_rounded_minus_adjusted_quantity AS quantity_rounded_minus_quantity_rounded_minus_adjusted_quantity
    FROM filtered_inventory
    WHERE quantity_rounded_minus_adjusted_quantity > 28
),
enriched AS (
    -- Attach products attributes
    SELECT
        a.*,
        b.category,
        b.stock_qty,
        b.product_name
    FROM filtered_inventory_2 a
    LEFT JOIN products b
        ON a.product_id = b.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity_rounded,
        adjusted_quantity,
        quantity_rounded_filled,
        category,
        ROW_NUMBER() OVER (PARTITION BY product_id, category ORDER BY quantity_rounded_minus_adjusted_quantity DESC, id) AS row_seq,
        LEAD(quantity_rounded_minus_adjusted_quantity, 1, 0) OVER (PARTITION BY product_id ORDER BY quantity_rounded, id) AS next_quantity_rounded_minus_adjusted_quantity,
        AVG(quantity_rounded) OVER (PARTITION BY category ORDER BY quantity_rounded_minus_quantity_rounded_minus_adjusted_quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_rounded_avg_2,
        LEAD(adjusted_quantity, 1, 0) OVER (PARTITION BY category ORDER BY quantity_rounded, id) AS next_adjusted_quantity
    FROM enriched
)
SELECT
    id,
    quantity_rounded,
    adjusted_quantity,
    quantity_rounded_filled,
    category,
    row_seq,
    next_quantity_rounded_minus_adjusted_quantity,
    quantity_rounded_avg_2,
    CASE WHEN category RLIKE '^(Grocery|Fashion)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS category_zone
FROM metrics
ORDER BY category_zone DESC NULLS FIRST;
