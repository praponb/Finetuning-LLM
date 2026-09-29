WITH valid_moves AS (
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
        LOWER(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM valid_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        operator,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_id
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM per_session
    WHERE duration_min > (SELECT MIN(duration_min) FROM per_session)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY session_id ORDER BY session_end) AS session_end_rank,
        SIZE(COLLECT_SET(operator) OVER w_session_end_year_session_id) AS distinct_operator_in_session_end_year_session_id,
        SUM(n_events) OVER (PARTITION BY YEAR(session_end), session_id) AS session_end_year_session_id_sum_n_events,
        ROUND(PERCENT_RANK() OVER w_session_id_by_session_end, 3) AS session_end_percent_rank
    FROM filtered
    WINDOW w_session_end_year_session_id AS (PARTITION BY YEAR(session_end), session_id), w_session_id_by_session_end AS (PARTITION BY session_id ORDER BY session_end, operator, session_id)
)
SELECT
    operator,
    session_id,
    session_start,
    n_events,
    session_end_rank,
    distinct_operator_in_session_end_year_session_id,
    session_end_year_session_id_sum_n_events,
    session_end_percent_rank,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
    CASE WHEN operator IN ('op_dew', 'op_chan') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group
FROM windowed
WHERE session_end_year_session_id_sum_n_events < 54
ORDER BY session_id NULLS LAST
