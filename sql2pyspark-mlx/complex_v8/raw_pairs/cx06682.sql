WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
),
filtered_2 AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.source IN ('radio', 'album')
              AND HOUR(p.played_at) BETWEEN 6 AND 8
        )
),
rollup_base AS (
    -- Summarise per group
    SELECT
        track_id,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played,
        COUNT(*) AS n_rows
    FROM filtered_2
    GROUP BY track_id
),
enriched_track AS (
    -- Enrich with dimension data
    SELECT
        s.track_id,
        s.n_rows,
        t.bpm AS track_bpm,
        t.duration_sec AS track_duration_sec,
        t.moods AS track_moods
    FROM rollup_base s
    JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(track_duration_sec) OVER (PARTITION BY track_id) AS track_id_min_track_duration_sec,
        COUNT(*) OVER (PARTITION BY track_id, n_rows ORDER BY n_rows DESC, track_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM enriched_track
)
SELECT
    track_id,
    track_bpm,
    track_duration_sec,
    track_moods,
    track_id_min_track_duration_sec,
    SIZE(track_moods) AS track_moods_count,
    SIZE(track_moods) AS track_moods_count_2
FROM metrics
ORDER BY track_duration_sec, track_id
LIMIT 5
