WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.ms_played) AS prior_ms_played_3d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
combined_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_3d,
        l.username AS listener_username,
        l.plan AS listener_plan
    FROM covered s
    JOIN listeners l ON l.id = s.listener_id
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.listener_username,
        s.listener_plan,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device
    FROM combined_details_listener s
    JOIN plays p ON p.listener_id = s.listener_id AND p.device IN ('speaker', 'car')
),
per_group AS (
    -- Group-level totals
    SELECT
        listener_id,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(listener_username)) LIKE 'E%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_listener_username,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT play_source) AS distinct_play_sources,
        COUNT(DISTINCT play_id) AS distinct_play_ids,
        COUNT(*) FILTER (WHERE UPPER(TRIM(listener_username)) LIKE 'E%') AS n_upper_trim_listener_username
    FROM line_level_plays
    GROUP BY listener_id
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    listener_id,
    n_rows,
    distinct_play_sources,
    distinct_play_ids,
    n_upper_trim_listener_username,
    FLOOR(n_rows / 2) * 2 AS n_rows_bucket
FROM per_group
ORDER BY distinct_play_sources, listener_id
LIMIT 10
