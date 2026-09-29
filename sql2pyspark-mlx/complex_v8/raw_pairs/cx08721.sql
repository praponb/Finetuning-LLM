WITH base AS (
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
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        operator,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(unit_price) AS session_unit_price,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        n_events,
        COUNT(DISTINCT operator) AS distinct_operators,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM session_summary
    GROUP BY 1
    HAVING MAX(session_unit_price) > 200.9
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(distinct_operators) OVER (PARTITION BY n_events, distinct_operators ORDER BY distinct_operators, n_events ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS distinct_operators_avg_2,
        MIN(distinct_operators) OVER (PARTITION BY n_events, distinct_operators) AS n_events_distinct_operators_min_distinct_operato,
        ROUND(distinct_operators - AVG(distinct_operators) OVER (PARTITION BY n_events, distinct_operators), 2) AS distinct_operators_vs_n_events_distinct_operator
    FROM rollup_base
)
SELECT
    n_events,
    distinct_operators,
    operator_list,
    distinct_operators_avg_2,
    n_events_distinct_operators_min_distinct_operato,
    distinct_operators_vs_n_events_distinct_operator,
    ROUND(distinct_operators / NULLIF(distinct_operators_avg_2, 0), 3) AS distinct_operators_per_distinct_operators_avg_2
FROM ranked
WHERE distinct_operators_avg_2 <= 6.8
ORDER BY distinct_operators_vs_n_events_distinct_operator NULLS LAST, n_events
LIMIT 5
