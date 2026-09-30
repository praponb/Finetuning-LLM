WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND UPPER(TRIM(title)) LIKE 'P%'
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm_bucket,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        a.artist_name
    FROM valid s
    LEFT JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER w_release_date_year_by_artist AS artist_debut_year_tercile,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit) AS rows_in_release_date_month_is_explicit,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY bpm_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        LAG(artist_debut_year) OVER (PARTITION BY is_explicit ORDER BY bpm_bucket ASC NULLS LAST, id) AS prev_artist_debut_year,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit), 0), 4) AS artist_debut_year_share_of_release_date_month_is,
        ROUND(AVG(bpm_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit), 2) AS release_date_month_is_explicit_avg_bpm_bucket,
        LAST_VALUE(bpm_bucket) OVER (PARTITION BY YEAR(release_date) ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm_bucket
    FROM combined_details_artist
    WINDOW w_release_date_year_by_artist AS (PARTITION BY YEAR(release_date) ORDER BY artist_debut_year, id), w_release_date_month_is_explic AS (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit)
)
SELECT
    id,
    release_date,
    is_explicit,
    bpm_bucket,
    artist_country,
    artist_name,
    last_known_release_date,
    prev_artist_debut_year,
    artist_debut_year_share_of_release_date_month_is,
    release_date_month_is_explicit_avg_bpm_bucket,
    last_bpm_bucket,
    QUARTER(last_known_release_date) AS last_known_release_date_quarter,
    COALESCE(artist_debut_year, 0) AS artist_debut_year_filled
FROM ranked
ORDER BY prev_artist_debut_year, release_date DESC, id
LIMIT 20
