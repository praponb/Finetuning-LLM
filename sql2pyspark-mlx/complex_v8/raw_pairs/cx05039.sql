WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'C%'
      AND release_date < DATE '2025-03-08'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        days_since_release_date,
        ROW_NUMBER() OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm NULLS LAST, id) AS rn,
        MIN(duration_sec) OVER w_release_date_month_is_explic AS release_date_month_is_explicit_min_duration_sec,
        NTH_VALUE(days_since_release_date, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_release_date,
        days_since_release_date - LAG(days_since_release_date) OVER w_artist_id_is_explicit_by_bpm AS days_since_release_date_change,
        ROUND(days_since_release_date - AVG(days_since_release_date) OVER (PARTITION BY MONTH(release_date), is_explicit), 2) AS days_since_release_date_vs_release_date_month_is
    FROM base
    WINDOW w_release_date_month_is_explic AS (PARTITION BY MONTH(release_date), is_explicit), w_artist_id_is_explicit_by_bpm AS (PARTITION BY artist_id, is_explicit ORDER BY bpm NULLS LAST, id)
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        COUNT(*) AS play_count,
        MIN(ms_played) AS min_play_ms_played
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played,
        COALESCE(p.play_count, 0) AS play_count,
        p.min_play_ms_played
    FROM windowed s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.device NOT IN ('tv', 'speaker')
        )
)
SELECT
    rn,
    play_count,
    is_explicit,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids
FROM with_activity
GROUP BY ROLLUP(rn, play_count, is_explicit)
ORDER BY rn NULLS LAST, play_count NULLS LAST, is_explicit NULLS LAST
