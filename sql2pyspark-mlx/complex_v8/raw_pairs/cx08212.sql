WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        ABS(duration_sec - 183) AS duration_sec_distance,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'chill')
      AND UPPER(TRIM(title)) LIKE 'M%'
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.days_since_release_date,
        s.bpm_bucket,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.country IN ('JP', 'US')
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        QUARTER(release_date) AS release_date_quarter,
        days_since_release_date + bpm AS days_since_release_date_plus_bpm,
        IF(artist_is_verified, 'yes', 'no') AS artist_is_verified_label
    FROM joined
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN artist_country IN ('US', 'BR') THEN 'group_a' WHEN artist_country = 'TH' THEN 'group_b' ELSE 'other' END AS artist_country_group
    FROM derived
)
SELECT
    artist_is_verified,
    is_explicit,
    artist_country,
    COUNT(*) AS n_rows,
    ROUND(STDDEV(release_date_quarter), 2) AS stddev_release_date_quarter,
    SUM(CASE WHEN is_explicit = FALSE THEN 1 ELSE 0 END) AS n_is_explicit_false
FROM labeled
GROUP BY GROUPING SETS ((artist_is_verified, is_explicit, artist_country), (artist_is_verified))
ORDER BY artist_is_verified NULLS LAST, is_explicit NULLS LAST, artist_country NULLS LAST
