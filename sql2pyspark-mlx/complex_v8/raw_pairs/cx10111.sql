WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m,
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE operator IS NOT NULL
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM recent_moves s
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        move_count,
        ROUND(SUM(move_count) / NULLIF(SUM(qty), 0), 3) AS move_count_to_qty_ratio,
        ROUND(100 * COUNT(CASE WHEN move_type <> 'INBOUND' THEN 1 END) / COUNT(*), 1) AS pct_move_type_inbound,
        ROUND(AVG(qty), 2) AS avg_qty,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        MAX(qty) AS max_qty,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids
    FROM profiled
    GROUP BY 1
    HAVING SUM(move_count) > 2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        move_count,
        move_count_to_qty_ratio,
        pct_move_type_inbound,
        distinct_move_types,
        max_qty,
        DENSE_RANK() OVER (ORDER BY avg_qty ASC) AS avg_qty_dense_rank,
        COUNT(*) OVER (PARTITION BY move_count) AS rows_in_move_count,
        LAG(distinct_sku_ids, 2) OVER w_all_by_avg_qty AS prev2_distinct_sku_ids,
        ROUND(avg_qty / NULLIF(SUM(avg_qty) OVER (PARTITION BY move_count), 0), 4) AS avg_qty_share_of_move_count
    FROM summary
    WINDOW w_move_count AS (PARTITION BY move_count), w_all_by_avg_qty AS (ORDER BY avg_qty ASC, move_count)
)
-- Final projection
SELECT
    move_count,
    move_count_to_qty_ratio,
    pct_move_type_inbound,
    avg_qty_dense_rank,
    rows_in_move_count,
    prev2_distinct_sku_ids,
    avg_qty_share_of_move_count,
    FLOOR(rows_in_move_count / 5) * 5 AS rows_in_move_count_bucket
FROM sequenced
WHERE avg_qty_share_of_move_count > 0.2
ORDER BY pct_move_type_inbound DESC NULLS FIRST
