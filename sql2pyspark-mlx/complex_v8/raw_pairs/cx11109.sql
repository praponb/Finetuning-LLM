WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_ADD(release_date, 30) AS release_date_plus_30d,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE release_date >= DATE '2024-10-23'
      AND bpm <= 156
),
summary AS (
    -- Group-level totals
    SELECT
        artist_id,
        DATE_FORMAT(release_date_plus_30d, 'yyyy-MM') AS release_date_plus_30d_ym,
        is_explicit,
        SUM(CASE WHEN is_explicit = TRUE THEN 1 ELSE 0 END) AS n_is_explicit_true,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS n_is_explicit_true_2,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        MAX(release_date_plus_30d) AS last_release_date_plus_30d
    FROM base
    GROUP BY 1, 2, 3
    HAVING MAX(bpm) > 89
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM summary s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm <= 170
              AND ARRAY_CONTAINS(t.moods, 'sad')
        )
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.artist_id,
        s.release_date_plus_30d_ym,
        s.is_explicit,
        s.n_is_explicit_true,
        s.n_is_explicit_true_2,
        s.distinct_artist_ids,
        s.last_release_date_plus_30d,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country,
        a.artist_name
    FROM without_matches s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year > 1994
)
-- Native SQL UNPIVOT (wide to long)
SELECT artist_id, release_date_plus_30d_ym, metric, metric_value
FROM (
    SELECT artist_id, release_date_plus_30d_ym, n_is_explicit_true, artist_debut_year, distinct_artist_ids
    FROM enriched
) src
UNPIVOT (
    metric_value FOR metric IN (n_is_explicit_true, artist_debut_year, distinct_artist_ids)
) unp
