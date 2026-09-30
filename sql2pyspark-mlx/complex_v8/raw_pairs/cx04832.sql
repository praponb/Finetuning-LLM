WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE listener_id IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        ABS(ms_played - 223828) AS ms_played_distance
    FROM recent
),
source_stats AS (
    -- Median ms_played_filled per source
    SELECT source, PERCENTILE_APPROX(ms_played_filled, 0.5) AS median_ms_played_filled
    FROM derived
    GROUP BY source
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played_filled,
        ROUND(s.ms_played_filled - g.median_ms_played_filled, 2) AS ms_played_filled_minus_median
    FROM derived s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played_filled >= g.median_ms_played_filled
),
combined_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.ms_played_filled,
        s.source_or_unknown,
        s.median_ms_played_filled,
        s.ms_played_filled_minus_median,
        t.artist_id AS track_artist_id,
        t.bpm AS track_bpm,
        t.duration_sec AS track_duration_sec,
        l.birth_year AS listener_birth_year
    FROM vs_group s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(ms_played_filled) OVER (PARTITION BY is_skipped ORDER BY ms_played_filled_minus_median DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played_filled,
        MAX(track_duration_sec) OVER (PARTITION BY source_or_unknown, track_id ORDER BY track_duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS track_duration_sec_max_3,
        ROUND(CUME_DIST() OVER (PARTITION BY source_or_unknown, track_id ORDER BY track_duration_sec, id), 3) AS track_duration_sec_cume_dist
    FROM combined_details
),
rollup_base AS (
    -- Group-level totals
    SELECT
        track_artist_id,
        source_or_unknown,
        SUM(track_bpm) AS total_track_bpm,
        ROUND(AVG(listener_birth_year), 2) AS avg_listener_birth_year
    FROM ranked
    WHERE listener_id IS NOT NULL
    GROUP BY 1, 2
)
SELECT *
FROM (
    SELECT track_artist_id, source_or_unknown, total_track_bpm
    FROM rollup_base
) src
PIVOT (
    AVG(total_track_bpm)
    FOR source_or_unknown IN ('UNKNOWN', 'playlist', 'radio', 'search')
)
ORDER BY track_artist_id
