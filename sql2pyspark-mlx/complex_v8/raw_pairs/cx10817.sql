WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        ABS(duration_sec - 344) AS duration_sec_distance,
        COALESCE(bpm, 0) AS bpm_filled
    FROM tracks
    WHERE (UPPER(TRIM(title)) LIKE 'E%' OR duration_sec >= 209)
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.duration_sec_distance,
        s.bpm_filled,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre = 'HipHop'
),
qualified AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM with_details_artist s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.release_date >= DATE '2024-11-06'
              AND t.bpm >= 85
        )
),
with_parent AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm_filled,
        s.artist_genre,
        s.artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM qualified s
    JOIN artists a ON a.id = s.artist_id
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.source IN ('search', 'playlist', 'radio')
              AND p.listener_id <= 9
        )
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        is_explicit,
        artist_is_verified,
        ROUND(100 * COUNT(CASE WHEN is_explicit = TRUE THEN 1 END) / COUNT(*), 1) AS pct_is_explicit_true,
        SUM(bpm_filled) AS total_bpm_filled,
        ARRAY_SORT(COLLECT_SET(artist_genre)) AS artist_genre_list
    FROM with_activity
    GROUP BY is_explicit, artist_is_verified
    HAVING AVG(artist_debut_year) > 1997
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(total_bpm_filled) OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY pct_is_explicit_true DESC, is_explicit, artist_is_verified) AS next_total_bpm_filled,
        RANK() OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY pct_is_explicit_true DESC, is_explicit, artist_is_verified) AS pct_is_explicit_true_rank,
        ROUND(AVG(total_bpm_filled) OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY pct_is_explicit_true DESC, is_explicit, artist_is_verified ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS total_bpm_filled_avg_7
    FROM per_group
)
SELECT
    is_explicit,
    pct_is_explicit_true_rank,
    artist_is_verified,
    MIN(pct_is_explicit_true_rank) AS min_pct_is_explicit_true_rank,
    PERCENTILE_APPROX(pct_is_explicit_true, 0.5) AS median_pct_is_explicit_true
FROM metrics
GROUP BY GROUPING SETS ((is_explicit, pct_is_explicit_true_rank, artist_is_verified), (is_explicit), ())
ORDER BY is_explicit NULLS LAST, pct_is_explicit_true_rank NULLS LAST, artist_is_verified NULLS LAST
