WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        QUARTER(release_date) AS release_date_quarter,
        ADD_MONTHS(release_date, 12) AS release_date_plus_12m
    FROM tracks
    WHERE is_explicit = TRUE
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND SIZE(t.moods) >= 2
        )
      AND s.artist_id <= 6
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.release_date_quarter,
        s.release_date_plus_12m,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM screened s
    LEFT JOIN artists a ON a.id = s.artist_id
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(release_date_plus_12m, 'yyyy-MM') AS release_date_plus_12m_ym,
        release_date_quarter,
        PERCENTILE_APPROX(artist_debut_year, 0.5) AS median_artist_debut_year,
        SUM(CASE WHEN NOT is_explicit THEN release_date_quarter ELSE 0 END) AS release_date_quarter_not_is_explicit,
        COUNT(*) AS row_count
    FROM with_parent
    WHERE artist_country = 'KR'
    GROUP BY 1, 2
),
labeled AS (
    -- Derive helper columns
    SELECT
        release_date_plus_12m_ym,
        release_date_quarter,
        median_artist_debut_year,
        release_date_quarter_not_is_explicit,
        row_count,
        row_count - median_artist_debut_year AS row_count_minus_median_artist_debut_year,
        INITCAP(TRIM(release_date_plus_12m_ym)) AS release_date_plus_12m_ym_clean
    FROM stats
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE median_artist_debut_year > (SELECT AVG(median_artist_debut_year) FROM labeled)
),
enriched AS (
    -- Add calculated columns
    SELECT
        release_date_plus_12m_ym,
        release_date_quarter,
        median_artist_debut_year,
        release_date_quarter_not_is_explicit,
        row_count,
        release_date_plus_12m_ym_clean,
        FLOOR(median_artist_debut_year / 5) * 5 AS median_artist_debut_year_bucket
    FROM filtered
)
SELECT
    release_date_plus_12m_ym,
    median_artist_debut_year,
    row_count,
    median_artist_debut_year_bucket
FROM enriched
