WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        DATE_TRUNC('QUARTER', moved_at) AS moved_at_quarter
    FROM stock_moves
    WHERE qty < 384
      AND operator IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        CONCAT(move_type, '#', CAST(id AS STRING)) AS move_type_ref
    FROM prepared
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM labeled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type = 'TRANSFER'
              AND st.operator NOT IN ('op_chan', 'op_bee')
        )
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.move_type,
        s.move_type_ref,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM filtered s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.opened_on BETWEEN DATE '2024-11-25' AND DATE '2025-12-17'
    WHERE w.opened_on IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        move_type_ref,
        warehouse_city,
        warehouse_opened_on,
        LEAD(warehouse_opened_on) OVER (PARTITION BY move_type, warehouse_id ORDER BY warehouse_opened_on ASC, id) AS next_warehouse_opened_on
    FROM combined_details
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(next_warehouse_opened_on) AS next_warehouse_opened_on_year,
        DATEDIFF(MAX(next_warehouse_opened_on), MIN(next_warehouse_opened_on)) AS next_warehouse_opened_on_span_days,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(warehouse_opened_on) AS last_warehouse_opened_on,
        COUNT(*) AS n_rows
    FROM sequenced
    GROUP BY YEAR(next_warehouse_opened_on)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        next_warehouse_opened_on_year,
        next_warehouse_opened_on_span_days,
        distinct_ids,
        last_warehouse_opened_on,
        n_rows,
        COUNT(*) OVER (PARTITION BY n_rows, next_warehouse_opened_on_year) AS rows_in_n_rows_next_warehouse_opened_on_year,
        ROUND(distinct_ids / NULLIF(SUM(distinct_ids) OVER (PARTITION BY n_rows, next_warehouse_opened_on_year), 0), 4) AS distinct_ids_share_of_n_rows_next_warehouse_open,
        DENSE_RANK() OVER w_distinct_ids_next_warehouse AS next_warehouse_opened_on_span_days_dense_rank
    FROM totals
    WINDOW w_distinct_ids_next_warehouse AS (PARTITION BY distinct_ids, next_warehouse_opened_on_year ORDER BY next_warehouse_opened_on_span_days NULLS LAST, next_warehouse_opened_on_year)
)
-- Final projection
SELECT
    next_warehouse_opened_on_year,
    next_warehouse_opened_on_span_days,
    rows_in_n_rows_next_warehouse_opened_on_year,
    ROUND(distinct_ids / NULLIF(n_rows, 0), 3) AS distinct_ids_per_n_rows
FROM metrics
WHERE distinct_ids_share_of_n_rows_next_warehouse_open BETWEEN 0.1 AND 0.8
ORDER BY distinct_ids_per_n_rows, next_warehouse_opened_on_year
LIMIT 10
