WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        CASE WHEN source IN ('radio', 'playlist') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE (listener_id IS NOT NULL OR source = 'search')
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND MONTH(p.played_at) IN (2, 3)
        )
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        source_or_unknown,
        source_group,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        COALESCE(source_or_unknown, 'UNKNOWN') AS source_or_unknown_or_unknown,
        played_at + INTERVAL 15 MINUTES AS played_at_plus_15m
    FROM without_matches
),
combined_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.source_or_unknown,
        s.days_since_played_at,
        s.played_at_plus_15m,
        t.release_date AS track_release_date,
        l.username AS listener_username
    FROM labeled s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        days_since_played_at,
        played_at_plus_15m,
        track_release_date,
        listener_username,
        LAG(ms_played, 1, 0) OVER w_all_by_ms_played AS prev_ms_played,
        LAG(days_since_played_at, 3, 0) OVER (ORDER BY ms_played ASC NULLS LAST, id) AS prev3_days_since_played_at,
        ms_played - LAG(ms_played) OVER (ORDER BY ms_played ASC NULLS LAST, id) AS ms_played_change,
        ROUND(days_since_played_at - AVG(days_since_played_at) OVER (PARTITION BY listener_id, track_id), 2) AS days_since_played_at_vs_listener_id_track_id_avg,
        ROUND((UNIX_TIMESTAMP(played_at_plus_15m) - UNIX_TIMESTAMP(LAG(played_at_plus_15m) OVER (ORDER BY ms_played ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_played_at_plus_15m
    FROM combined_details_track
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played ASC NULLS LAST, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        ms_played,
        source,
        played_at_plus_15m,
        listener_username,
        days_since_played_at_vs_listener_id_track_id_avg,
        minutes_since_prev_played_at_plus_15m,
        CONCAT(listener_username, '#', CAST(prev_ms_played AS STRING)) AS listener_username_ref,
        DATE_ADD(track_release_date, 90) AS track_release_date_plus_90d,
        COALESCE(ms_played_change, 0) AS ms_played_change_filled
    FROM windowed
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(ms_played_change_filled) IGNORE NULLS OVER (ORDER BY ms_played_change_filled ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played_change_filled,
        MAX(ms_played) OVER (PARTITION BY listener_id, MONTH(played_at_plus_15m)) AS listener_id_played_at_plus_15m_month_max_ms_play,
        LAG(ms_played, 2) OVER (ORDER BY ms_played_change_filled ASC NULLS LAST, id) AS prev2_ms_played
    FROM enriched
)
-- Final projection
SELECT
    id,
    listener_id,
    ms_played,
    source,
    played_at_plus_15m,
    listener_username,
    days_since_played_at_vs_listener_id_track_id_avg,
    minutes_since_prev_played_at_plus_15m,
    listener_username_ref,
    track_release_date_plus_90d,
    ms_played_change_filled,
    first_known_ms_played_change_filled,
    listener_id_played_at_plus_15m_month_max_ms_play,
    prev2_ms_played,
    CASE WHEN source IN ('playlist', 'album') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group,
    COALESCE(source, 'UNKNOWN') AS source_or_unknown
FROM metrics
ORDER BY first_known_ms_played_change_filled DESC NULLS FIRST, source_or_unknown
