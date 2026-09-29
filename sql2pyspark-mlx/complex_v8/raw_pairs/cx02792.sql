WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        unit_price,
        CASE WHEN move_type IN ('INBOUND', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM base_moves
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(unit_price) OVER w_move_type, 2) AS move_type_avg_unit_price,
        NTILE(5) OVER (PARTITION BY move_type_group, warehouse_id ORDER BY unit_price, id) AS unit_price_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type_group ORDER BY unit_price), 3) AS unit_price_cume_dist,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type
    FROM derived
    WINDOW w_move_type AS (PARTITION BY move_type)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        move_type,
        SUM(CASE WHEN move_type NOT IN ('TRANSFER', 'ADJUST') THEN 1 ELSE 0 END) AS n_move_type_not_in_transfer_ad,
        ROUND(100 * COUNT(CASE WHEN move_type = 'OUTBOUND' THEN 1 END) / COUNT(*), 1) AS pct_move_type_outbound,
        SUM(CASE WHEN move_type IN ('TRANSFER', 'ADJUST', 'OUTBOUND') THEN 1 ELSE 0 END) AS n_move_type_in_transfer_adjust,
        MAX(unit_price_cume_dist) AS max_unit_price_cume_dist
    FROM windowed
    WHERE rows_in_move_type >= 10
    GROUP BY 1
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN max_unit_price_cume_dist >= 0.7 THEN 'A' WHEN max_unit_price_cume_dist >= 0.2 THEN 'B' ELSE 'C' END AS max_unit_price_cume_dist_tier
    FROM stats
    WHERE n_move_type_in_transfer_adjust < 8
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE n_move_type_not_in_transfer_ad > (SELECT AVG(n_move_type_not_in_transfer_ad) FROM scored)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE n_move_type_in_transfer_adjust > (SELECT AVG(n_move_type_in_transfer_adjust) FROM with_activity)
)
SELECT
    move_type,
    n_move_type_not_in_transfer_ad,
    n_move_type_in_transfer_adjust,
    max_unit_price_cume_dist,
    max_unit_price_cume_dist_tier,
    COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
    ROUND(pct_move_type_outbound / NULLIF(max_unit_price_cume_dist, 0), 3) AS pct_move_type_outbound_per_max_unit_price_cume_d
FROM without_matches
ORDER BY pct_move_type_outbound_per_max_unit_price_cume_d
