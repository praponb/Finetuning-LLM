WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        sku_id,
        unit_price,
        moved_at_date,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM recent
),
enriched_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.sku_id,
        s.unit_price,
        s.moved_at_date,
        s.unit_price_filled,
        sk.category AS sku_category
    FROM scored s
    JOIN skus sk ON sk.id = s.sku_id
),
per_group AS (
    -- Group-level totals
    SELECT
        sku_id,
        sku_category,
        DATE_TRUNC('MONTH', moved_at_date) AS moved_at_date_month,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        MAX(moved_at_date) AS last_moved_at_date
    FROM enriched_sku
    GROUP BY sku_id, sku_category, DATE_TRUNC('MONTH', moved_at_date)
),
scored_2 AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(sku_category)) AS sku_category_clean,
        SUBSTRING(TRIM(sku_category), 1, 2) AS sku_category_prefix
    FROM per_group
    WHERE last_moved_at_date BETWEEN DATE '2024-08-01' AND DATE '2025-08-01'
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN cnt >= 10 THEN 'HIGH' WHEN cnt >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS cnt_tier
    FROM scored_2
    WHERE last_moved_at_date BETWEEN DATE '2024-02-01' AND DATE '2025-05-01'
)
SELECT
    sku_id,
    sku_category,
    distinct_ids,
    cnt,
    sku_category_clean,
    sku_category_prefix,
    CONCAT_WS(' / ', cnt_tier, sku_category_prefix) AS cnt_tier_sku_category_prefix_label
FROM derived
