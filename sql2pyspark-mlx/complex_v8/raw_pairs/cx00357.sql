WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        CASE WHEN source IN ('album', 'radio') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group,
        DATE_TRUNC('MONTH', played_at) AS played_at_month
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-12 09:00:00'
),
joined_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source_group,
        t.moods AS track_moods,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre
    FROM base s
    JOIN tracks t ON t.id = s.track_id AND SIZE(t.moods) >= 2
    LEFT JOIN artists a ON a.id = t.artist_id
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM joined_track s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.played_at >= TIMESTAMP '2025-02-06 12:00:00'
              AND p.ms_played >= 95960
        )
),
scored AS (
    -- Label and score rows
    SELECT *, CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM screened
)
-- Subtotals with GROUPING SETS
SELECT
    track_id,
    is_skipped,
    GROUPING(track_id) AS is_total_track_id,
    PERCENTILE_APPROX(is_skipped_flag, 0.5) AS median_is_skipped_flag,
    COUNT(DISTINCT artist_genre) AS distinct_artist_genres
FROM scored
GROUP BY GROUPING SETS ((track_id, is_skipped), (track_id), ())
