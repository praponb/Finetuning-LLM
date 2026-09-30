WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(bpm, 0) AS bpm_filled
    FROM tracks
    WHERE (duration_sec BETWEEN 128 AND 352 OR is_explicit = FALSE)
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MIN(played_at) AS first_played_at,
        MIN(played_at) AS first_played_at_2
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        p.first_played_at_2
    FROM filtered_tracks s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country,
        a.artist_name
    FROM summarized_rows s
    JOIN artists a ON a.id = s.artist_id
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_details_artist s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.device = 'desktop'
              AND p.source IS NOT NULL
        )
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id, is_explicit ORDER BY first_played_at_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        ROUND(AVG(bpm_filled) OVER (PARTITION BY artist_country, artist_id), 2) AS artist_country_artist_id_avg_bpm_filled,
        NTH_VALUE(first_played_at_2, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY first_played_at_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_played_at_2
    FROM screened
),
screened_2 AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND UPPER(TRIM(t.title)) LIKE 'G%'
        )
)
SELECT
    id,
    artist_id,
    is_explicit,
    bpm,
    artist_debut_year,
    artist_country,
    artist_name,
    first_known_release_date,
    artist_country_artist_id_avg_bpm_filled,
    second_first_played_at_2,
    CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
FROM screened_2
ORDER BY first_known_release_date
