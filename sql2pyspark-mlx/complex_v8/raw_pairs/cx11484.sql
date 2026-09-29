WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE release_date IS NOT NULL
      AND UPPER(TRIM(title)) LIKE 'R%'
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN bpm >= 147 THEN 'A' WHEN bpm >= 96 THEN 'B' ELSE 'C' END AS bpm_tier,
        COALESCE(bpm, 0) AS bpm_filled,
        ADD_MONTHS(release_date, 12) AS release_date_plus_12m
    FROM base_tracks
    WHERE bpm > 76
),
with_details AS (
    -- Join artists
    SELECT s.*, a.country AS artist_country, a.artist_name
    FROM annotated s
    LEFT JOIN artists a ON a.id = s.artist_id
),
per_group AS (
    -- Summarise per group
    SELECT
        artist_id,
        artist_country,
        ROUND(AVG(bpm), 2) AS avg_bpm,
        MIN(release_date_plus_12m) AS first_release_date_plus_12m,
        ROUND(STDDEV(bpm), 2) AS stddev_bpm,
        SUM(CASE WHEN artist_country <> 'US' THEN bpm ELSE 0 END) AS bpm_artist_country_us,
        COUNT(*) FILTER (WHERE UPPER(TRIM(artist_name)) LIKE 'S%') AS n_upper_trim_artist_name_like,
        MAX(release_date_plus_12m) AS last_release_date_plus_12m
    FROM with_details
    GROUP BY 1, 2
    HAVING AVG(bpm_filled) > 78
)
-- Native SQL UNPIVOT (wide to long)
SELECT artist_id, artist_country, measure, reading
FROM (
    SELECT artist_id, artist_country, stddev_bpm, avg_bpm
    FROM per_group
) src
UNPIVOT (
    reading FOR measure IN (stddev_bpm AS m_stddev_bpm, avg_bpm AS avg_bpm_v)
) unp
ORDER BY artist_id, artist_country, measure
