WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        COALESCE(qty, 0) AS qty_filled,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label,
        CONCAT(operator, '#', CAST(qty AS STRING)) AS operator_ref
    FROM stock_moves
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        qty_filled,
        moved_at_label,
        operator_ref,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM prepared
),
latest_per_key AS (
    -- Keep the latest row per operator, move_type
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        qty_filled,
        moved_at_label,
        operator_ref,
        unit_price_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, move_type ORDER BY unit_price_bucket DESC, id DESC) AS recency_rank
        FROM labeled
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(qty_filled / NULLIF(SUM(qty_filled) OVER (PARTITION BY YEAR(moved_at), sku_id), 0), 4) AS qty_filled_share_of_moved_at_year_sku_id,
        SUM(qty) OVER (PARTITION BY YEAR(moved_at), sku_id) AS moved_at_year_sku_id_sum_qty
    FROM latest_per_key
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(qty_filled, qty) AS least_qty_filled_qty,
        CONCAT(moved_at_label, '#', CAST(id AS STRING)) AS moved_at_label_ref
    FROM ranked
    WHERE qty > 78
)
SELECT *
FROM (
    SELECT operator, move_type, moved_at_year_sku_id_sum_qty
    FROM enriched
) src
PIVOT (
    COUNT(moved_at_year_sku_id_sum_qty)
    FOR move_type IN ('ADJUST', 'TRANSFER', 'OUTBOUND')
)
