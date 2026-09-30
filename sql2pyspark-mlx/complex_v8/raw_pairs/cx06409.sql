WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE device = 'tv'
      AND listener_id IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        SUM(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played
    FROM cleaned_plays
),
scored AS (
    -- Derive helper columns
    SELECT *, TO_DATE(played_at) AS played_at_date
    FROM with_windows
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.release_date AS track_release_date,
        t.title AS track_title,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM scored s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
latest_per_key AS (
    -- Keep the latest row per source
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ms_played_avg_2,
        running_ms_played,
        played_at_date,
        track_release_date,
        track_title,
        artist_debut_year,
        artist_is_verified
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source ORDER BY ms_played_avg_2 DESC, id DESC) AS rn
        FROM joined
    ) ranked
    WHERE rn = 1
),
enriched_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.running_ms_played,
        s.played_at_date,
        s.track_release_date,
        s.track_title,
        s.artist_debut_year,
        s.artist_is_verified,
        t.bpm AS track_bpm
    FROM latest_per_key s
    JOIN tracks t ON t.id = s.track_id
)
-- Final projection
SELECT
    id,
    played_at,
    source,
    running_ms_played,
    track_release_date,
    track_title,
    artist_debut_year,
    artist_is_verified
FROM enriched_track
