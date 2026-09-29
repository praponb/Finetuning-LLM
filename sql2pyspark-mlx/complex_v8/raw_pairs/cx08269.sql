WITH filtered_warehouses AS (
    -- Filter warehouses
    SELECT *
    FROM warehouses
    WHERE city IS NOT NULL
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered_warehouses
    WHERE capacity > (SELECT AVG(capacity) FROM filtered_warehouses)
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) FILTER (WHERE operator = 'op_dew') AS move_operator_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_operator_count, 0) AS move_operator_count
    FROM without_matches s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY move_operator_count ORDER BY opened_on DESC, id) AS rn,
        NTILE(5) OVER w_move_operator_count_by_opene AS opened_on_quintile
    FROM with_counts
    WINDOW w_move_operator_count_by_opene AS (PARTITION BY move_operator_count ORDER BY opened_on DESC, id)
),
stats AS (
    -- Group-level totals
    SELECT
        move_operator_count,
        opened_on_quintile,
        rn,
        ROUND(SUM(move_operator_count) / NULLIF(SUM(opened_on_quintile), 0), 3) AS move_operator_count_to_opened_on_quintile_ratio,
        COUNT(*) AS row_count,
        ROUND(STDDEV(capacity), 2) AS stddev_capacity,
        MAX(move_operator_count) AS max_move_operator_count,
        MAX(opened_on_quintile) AS max_opened_on_quintile
    FROM with_windows
    GROUP BY 1, 2, 3
    HAVING SUM(rn) > 2
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(move_operator_count, 2) OVER (ORDER BY stddev_capacity ASC, move_operator_count, opened_on_quintile, rn) AS prev2_move_operator_count,
        ROUND(stddev_capacity - AVG(stddev_capacity) OVER (PARTITION BY opened_on_quintile), 2) AS stddev_capacity_vs_opened_on_quintile_avg,
        MAX(move_operator_count) OVER w_opened_on_quintile AS opened_on_quintile_max_move_operator_count,
        ROW_NUMBER() OVER (ORDER BY stddev_capacity ASC, move_operator_count, opened_on_quintile, rn) AS rn_2,
        MAX(row_count) OVER (ORDER BY stddev_capacity ASC, move_operator_count, opened_on_quintile, rn ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS row_count_max_2_prior,
        RANK() OVER w_all_by_stddev_capacity_ties AS stddev_capacity_rank
    FROM stats
    WINDOW w_opened_on_quintile AS (PARTITION BY opened_on_quintile), w_all_by_stddev_capacity_ties AS (ORDER BY stddev_capacity ASC)
),
derived AS (
    -- Derive helper columns
    SELECT
        move_operator_count,
        opened_on_quintile,
        rn,
        move_operator_count_to_opened_on_quintile_ratio,
        stddev_capacity,
        max_opened_on_quintile,
        stddev_capacity_vs_opened_on_quintile_avg,
        opened_on_quintile_max_move_operator_count,
        rn_2,
        row_count_max_2_prior,
        ROUND(stddev_capacity * 0.9, 2) AS stddev_capacity_adj,
        CASE WHEN opened_on_quintile >= 9 THEN 'A' WHEN opened_on_quintile >= 3 THEN 'B' ELSE 'C' END AS opened_on_quintile_tier,
        COALESCE(row_count_max_2_prior, 0) AS row_count_max_2_prior_filled
    FROM ranked
    WHERE stddev_capacity_vs_opened_on_quintile_avg > -14000.0
)
SELECT
    move_operator_count,
    opened_on_quintile,
    move_operator_count_to_opened_on_quintile_ratio,
    stddev_capacity,
    max_opened_on_quintile,
    stddev_capacity_vs_opened_on_quintile_avg,
    opened_on_quintile_max_move_operator_count,
    row_count_max_2_prior,
    stddev_capacity_adj,
    row_count_max_2_prior_filled,
    COALESCE(opened_on_quintile_tier, 'UNKNOWN') AS opened_on_quintile_tier_or_unknown,
    ABS(max_opened_on_quintile - 6) AS max_opened_on_quintile_distance
FROM derived
WHERE opened_on_quintile_tier = 'A'
ORDER BY stddev_capacity_adj NULLS LAST, row_count_max_2_prior
