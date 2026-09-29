WITH recent AS (
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
        ROUND(unit_price * 1.1, 2) AS unit_price_adj,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        UPPER(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE qty >= 102
      AND moved_at < TIMESTAMP '2025-03-08 11:00:00'
),
latest_per_key AS (
    -- Keep the earliest row per warehouse_id, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_adj,
        days_since_moved_at,
        operator_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty, id) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at,
        (SELECT ROUND(AVG(st.qty), 2) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS avg_move_qty
    FROM latest_per_key s
),
with_parent_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price_adj,
        s.days_since_moved_at,
        s.operator_clean,
        s.last_moved_at,
        s.first_moved_at,
        s.avg_move_qty,
        w.opened_on AS warehouse_opened_on,
        sk.category AS sku_category,
        sk.labels AS sku_labels
    FROM with_counts s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
    WHERE w.opened_on < DATE '2025-07-03'
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_parent_warehouse s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.unit_price < 293.2
        )
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        move_type,
        operator,
        unit_price_adj,
        days_since_moved_at,
        operator_clean,
        last_moved_at,
        avg_move_qty,
        warehouse_opened_on,
        sku_labels,
        ROUND((UNIX_TIMESTAMP(last_moved_at) - UNIX_TIMESTAMP(first_moved_at)) / 60, 1) AS minutes_first_moved_at_to_last_moved_at
    FROM filtered
    WHERE unit_price_adj <= 637.6
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        move_type,
        days_since_moved_at,
        operator_clean,
        last_moved_at,
        avg_move_qty,
        sku_labels,
        minutes_first_moved_at_to_last_moved_at,
        DENSE_RANK() OVER (PARTITION BY MONTH(last_moved_at) ORDER BY warehouse_opened_on, id) AS warehouse_opened_on_dense_rank,
        ROUND(days_since_moved_at / NULLIF(SUM(days_since_moved_at) OVER w_move_type_operator, 0), 4) AS days_since_moved_at_share_of_move_type_operator,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY MONTH(last_moved_at) ORDER BY warehouse_opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        ROW_NUMBER() OVER (PARTITION BY MONTH(last_moved_at) ORDER BY warehouse_opened_on, id) AS seq_no
    FROM enriched
    WINDOW w_move_type_operator AS (PARTITION BY move_type, operator)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_opened_on_dense_rank, second_warehouse_opened_on, move_type, days_since_moved_at_share_of_move_type_operator
    FROM windowed
) src
PIVOT (
    AVG(days_since_moved_at_share_of_move_type_operator)
    FOR move_type IN ('INBOUND', 'TRANSFER')
)
ORDER BY warehouse_opened_on_dense_rank, second_warehouse_opened_on
