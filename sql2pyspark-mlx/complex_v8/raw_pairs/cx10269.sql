WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE MONTH(played_at) IN (1, 2)
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) FILTER (WHERE source <> 'radio') AS play_source_count,
        COUNT(CASE WHEN is_skipped = TRUE THEN 1 END) AS play_is_skipped_count,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count,
        COALESCE(p.play_count, 0) AS play_count
    FROM cleaned s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM profiled
    WHERE ms_played < (SELECT MAX(ms_played) FROM profiled)
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        s.played_at_label,
        s.source_or_unknown,
        s.is_skipped_flag,
        s.play_is_skipped_count,
        s.play_count,
        l.username AS listener_username,
        l.country AS listener_country,
        l.interests AS listener_interests
    FROM without_matches s
    JOIN listeners l ON l.id = s.listener_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(device)) AS device_clean,
        CASE WHEN play_count >= 5 THEN 'A' WHEN play_count >= 2 THEN 'B' ELSE 'C' END AS play_count_tier
    FROM joined_listener
)
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    device,
    source_or_unknown,
    is_skipped_flag,
    play_is_skipped_count,
    play_count,
    listener_username,
    listener_interests,
    device_clean,
    play_count_tier
FROM labeled
WHERE play_is_skipped_count < 6
