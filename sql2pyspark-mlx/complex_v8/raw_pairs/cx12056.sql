WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN unit_price >= 290.4 THEN 'large' WHEN unit_price >= 160.8 THEN 'medium' ELSE 'small' END AS unit_price_tier,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE warehouse_id <= 3
),
enriched_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_tier,
        s.unit_price_filled,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM filtered_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        qty,
        operator,
        unit_price_tier,
        sku_labels,
        sku_category,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_2_prior,
        ROUND(unit_price_filled / NULLIF(SUM(unit_price_filled) OVER w_sku_category_sku_id, 0), 4) AS unit_price_filled_share_of_sku_category_sku_id,
        ROUND(CUME_DIST() OVER (ORDER BY unit_price_filled DESC), 3) AS unit_price_filled_cume_dist,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        SUM(unit_price_filled) OVER (PARTITION BY unit_price_tier, sku_id ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_filled,
        LEAD(unit_price_filled) OVER w_warehouse_id_sku_id_by_moved_3 AS next_unit_price_filled,
        NTH_VALUE(qty, 2) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_category, sku_id)) AS distinct_warehouse_id_in_sku_category_sku_id,
        MIN(unit_price) OVER w_sku_category_sku_id AS sku_category_sku_id_min_unit_price
    FROM enriched_sku
    WINDOW w_sku_category_sku_id AS (PARTITION BY sku_category, sku_id), w_warehouse_id_sku_id_by_moved_3 AS (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        operator,
        sku_labels,
        unit_price_filled_share_of_sku_category_sku_id,
        next_unit_price_filled,
        second_qty,
        distinct_warehouse_id_in_sku_category_sku_id,
        sku_category_sku_id_min_unit_price,
        ROUND(next_unit_price_filled - AVG(next_unit_price_filled) OVER (PARTITION BY sku_category, sku_id), 2) AS next_unit_price_filled_vs_sku_category_sku_id_av,
        COUNT(*) OVER (PARTITION BY sku_category, sku_id) AS rows_in_sku_category_sku_id,
        next_unit_price_filled - LAG(next_unit_price_filled) OVER (PARTITION BY unit_price_tier ORDER BY sku_category_sku_id_min_unit_price DESC, id) AS next_unit_price_filled_change,
        SIZE(COLLECT_SET(unit_price_tier) OVER w_sku_category_sku_id) AS distinct_unit_price_tier_in_sku_category_sku_id,
        LEAD(next_unit_price_filled) OVER w_unit_price_tier_by_sku_categ AS next_next_unit_price_filled
    FROM sequenced
    WINDOW w_sku_category_sku_id AS (PARTITION BY sku_category, sku_id), w_unit_price_tier_by_sku_categ AS (PARTITION BY unit_price_tier ORDER BY sku_category_sku_id_min_unit_price DESC, id)
)
SELECT
    id,
    operator,
    sku_labels,
    next_unit_price_filled,
    second_qty,
    distinct_warehouse_id_in_sku_category_sku_id,
    sku_category_sku_id_min_unit_price,
    next_unit_price_filled_change,
    distinct_unit_price_tier_in_sku_category_sku_id,
    next_next_unit_price_filled,
    CASE WHEN second_qty >= 357 THEN 'large' WHEN second_qty >= 123 THEN 'medium' ELSE 'small' END AS second_qty_tier,
    SIZE(sku_labels) AS sku_labels_count
FROM metrics
WHERE distinct_unit_price_tier_in_sku_category_sku_id > 5
ORDER BY sku_category_sku_id_min_unit_price DESC NULLS FIRST, id DESC NULLS FIRST
