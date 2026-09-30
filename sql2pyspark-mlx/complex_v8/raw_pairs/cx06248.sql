WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label
    FROM plays
    WHERE device IN ('desktop', 'car')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id, listener_id), 0), 4) AS ms_played_share_of_track_id_listener_id,
        SUM(ms_played) OVER w_track_id_listener_id AS track_id_listener_id_sum_ms_played,
        NTILE(3) OVER w_all_by_ms_played AS ms_played_tercile,
        LEAD(ms_played) OVER (PARTITION BY is_skipped, track_id ORDER BY ms_played DESC, id) AS next_ms_played,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played
    FROM prepared_plays
    WINDOW w_track_id_listener_id AS (PARTITION BY track_id, listener_id), w_all_by_ms_played AS (ORDER BY ms_played, id)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, l.plan AS listener_plan
    FROM metrics s
    JOIN listeners l ON l.id = s.listener_id
)
-- Subtotals with CUBE
SELECT
    is_skipped,
    listener_id,
    SUM(ms_played) AS total_ms_played,
    PERCENTILE_APPROX(last_known_ms_played, 0.5) AS median_last_known_ms_played,
    ARRAY_SORT(COLLECT_SET(listener_plan)) AS listener_plan_list
FROM with_parent
GROUP BY CUBE(is_skipped, listener_id)
ORDER BY is_skipped NULLS LAST, listener_id NULLS LAST
