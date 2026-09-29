WITH recent AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(greatest_unit_price_qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_greatest_unit_price_qty,
        MAX(greatest_unit_price_qty) OVER w_sku_id AS sku_id_max_greatest_unit_price_qty,
        ROUND(CUME_DIST() OVER w_sku_id_by_qty_ties, 3) AS qty_cume_dist,
        ROUND(qty_bucket / NULLIF(SUM(qty_bucket) OVER (PARTITION BY sku_id), 0), 4) AS qty_bucket_share_of_sku_id
    FROM recent
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_sku_id_by_qty_ties AS (PARTITION BY sku_id ORDER BY qty)
),
with_parent AS (
    -- Attach sku attributes
    SELECT s.*, sk.labels AS sku_labels
    FROM with_trends s
    JOIN skus sk ON sk.id = s.sku_id
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        unit_price,
        greatest_unit_price_qty,
        qty_bucket,
        sku_id_max_greatest_unit_price_qty,
        qty_cume_dist,
        qty_bucket_share_of_sku_id,
        sku_labels,
        QUARTER(moved_at) AS moved_at_quarter
    FROM with_parent
    WHERE greatest_unit_price_qty < 375.2
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(qty_cume_dist) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY qty_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty_cume_dist,
        COUNT(*) OVER w_warehouse_id AS rows_in_warehouse_id,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_id)) AS distinct_operator_in_warehouse_id,
        NTH_VALUE(qty_cume_dist, 2) OVER (PARTITION BY sku_id ORDER BY qty_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_cume_dist,
        SUM(qty_bucket_share_of_sku_id) OVER w_warehouse_id AS warehouse_id_sum_qty_bucket_share_of_sku_id,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY qty_bucket DESC, id) AS seq_no
    FROM labeled
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, second_qty_cume_dist, sku_id_max_greatest_unit_price_qty
    FROM with_windows
) src
UNPIVOT (
    metric_value FOR metric_name IN (second_qty_cume_dist AS m_second_qty_cume_dist, sku_id_max_greatest_unit_price_qty AS sku_id_max_greatest_unit_price_qty_v)
) unp
ORDER BY id, metric_name
