WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE (source IN ('playlist', 'radio', 'album') OR ms_played > 72464)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_4,
        MAX(ms_played) OVER (PARTITION BY source) AS source_max_ms_played,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY source)) AS distinct_device_in_source,
        SUM(ms_played) OVER (PARTITION BY source, device ORDER BY ms_played DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ms_played_sum_4
    FROM scoped
),
per_group AS (
    -- Group-level totals
    SELECT
        track_id,
        is_skipped,
        COUNT(*) FILTER (WHERE NOT is_skipped) AS n_not_is_skipped,
        COUNT(*) FILTER (WHERE source IN ('playlist', 'radio', 'search')) AS n_source_in_playlist_radio_sea,
        ROUND(AVG(distinct_device_in_source), 2) AS avg_distinct_device_in_source
    FROM metrics
    GROUP BY track_id, is_skipped
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM per_group s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.ms_played > 160264
        )
),
screened_2 AS (
    -- Keep rows whose track matches
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.id = s.track_id
              AND SIZE(t.moods) >= 2
              AND t.duration_sec BETWEEN 144 AND 357
        )
),
enriched AS (
    -- Label and score rows
    SELECT
        track_id,
        is_skipped,
        n_not_is_skipped,
        COALESCE(n_not_is_skipped, 0) AS n_not_is_skipped_filled,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM screened_2
),
ranked AS (
    -- Previous / next values per key
    SELECT
        track_id,
        is_skipped,
        n_not_is_skipped,
        n_not_is_skipped_filled,
        is_skipped_flag,
        COUNT(*) OVER w_is_skipped_flag_n_not_is_ski AS rows_in_is_skipped_flag_n_not_is_skipped_filled,
        LEAD(is_skipped_flag, 1, 0) OVER w_track_id_is_skipped_flag_by AS next_is_skipped_flag,
        ROUND(is_skipped_flag / NULLIF(SUM(is_skipped_flag) OVER (PARTITION BY is_skipped_flag, n_not_is_skipped_filled), 0), 4) AS is_skipped_flag_share_of_is_skipped_flag_n_not_i
    FROM enriched
    WINDOW w_is_skipped_flag_n_not_is_ski AS (PARTITION BY is_skipped_flag, n_not_is_skipped_filled), w_track_id_is_skipped_flag_by AS (PARTITION BY track_id, is_skipped_flag ORDER BY n_not_is_skipped, track_id, is_skipped)
)
-- Final projection
SELECT
    track_id,
    is_skipped,
    n_not_is_skipped,
    is_skipped_flag,
    rows_in_is_skipped_flag_n_not_is_skipped_filled,
    next_is_skipped_flag,
    is_skipped_flag_share_of_is_skipped_flag_n_not_i,
    CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag_2,
    ABS(n_not_is_skipped_filled - 5) AS n_not_is_skipped_filled_distance
FROM ranked
ORDER BY n_not_is_skipped DESC NULLS FIRST, n_not_is_skipped_filled_distance DESC
