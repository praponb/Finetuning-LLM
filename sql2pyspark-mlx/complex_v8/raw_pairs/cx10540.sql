WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM stock_moves
    WHERE qty <= 299
),
deduped AS (
    -- Keep the earliest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        move_type_len,
        moved_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY move_type_len, id) AS recency_rank
        FROM base_moves
    ) ranked
    WHERE recency_rank = 1
),
unique_rows AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        move_type_len,
        moved_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC, id DESC) AS recency_rank
        FROM deduped
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label_2,
        ROUND(unit_price / NULLIF(move_type_len, 0), 3) AS unit_price_per_move_type_len,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM unique_rows
),
sku_id_stats AS (
    -- Avg unit_price per sku_id
    SELECT sku_id, AVG(unit_price) AS avg_unit_price
    FROM enriched
    GROUP BY sku_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price,
        ROUND(s.unit_price - g.avg_unit_price, 2) AS unit_price_minus_avg
    FROM enriched s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.unit_price > g.avg_unit_price
)
-- Subtotals with ROLLUP
SELECT
    warehouse_id,
    move_type_len,
    COALESCE(operator_or_unknown, 'ALL') AS operator_or_unknown,
    MIN(unit_price_minus_avg) AS min_unit_price_minus_avg,
    ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
FROM outliers
GROUP BY ROLLUP(warehouse_id, move_type_len, operator_or_unknown)
ORDER BY warehouse_id NULLS LAST, move_type_len NULLS LAST, operator_or_unknown NULLS LAST
