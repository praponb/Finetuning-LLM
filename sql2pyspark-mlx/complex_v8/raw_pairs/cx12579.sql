WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm
    FROM tracks
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        COUNT(*) FILTER (WHERE is_explicit = FALSE) AS n_is_explicit_false,
        MIN(release_date) AS first_release_date,
        ROUND(SUM(bpm) / NULLIF(SUM(duration_sec), 0), 3) AS bpm_to_duration_sec_ratio,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS n_is_explicit_true
    FROM scoped_tracks
    GROUP BY 1, 2
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND UPPER(TRIM(t.title)) LIKE 'M%'
              AND SIZE(t.moods) >= 2
        )
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.artist_id,
        s.release_date_ym,
        s.first_release_date,
        s.n_is_explicit_true,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM with_activity s
    LEFT JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY artist_id, n_is_explicit_true ORDER BY first_release_date, artist_id, release_date_ym) AS first_release_date_dense_rank,
        DENSE_RANK() OVER (PARTITION BY artist_is_verified, n_is_explicit_true ORDER BY first_release_date, artist_id, release_date_ym) AS first_release_date_dense_rank_2,
        MAX(n_is_explicit_true) OVER (PARTITION BY artist_is_verified) AS artist_is_verified_max_n_is_explicit_true
    FROM enriched_artist
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, COUNT(*) AS track_count
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count
    FROM ranked s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, a.genre AS artist_genre
    FROM profiled s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    artist_id,
    release_date_ym,
    n_is_explicit_true,
    artist_debut_year,
    artist_is_verified,
    first_release_date_dense_rank,
    first_release_date_dense_rank_2,
    artist_is_verified_max_n_is_explicit_true,
    artist_genre
FROM with_parent
ORDER BY first_release_date_dense_rank, release_date_ym DESC
