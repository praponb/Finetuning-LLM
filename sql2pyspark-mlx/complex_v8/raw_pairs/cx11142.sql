WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        ABS(duration_sec - 213) AS duration_sec_distance
    FROM tracks
    WHERE SIZE(moods) >= 1
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        a.country AS artist_country,
        a.artist_name
    FROM prepared s
    LEFT JOIN artists a ON a.id = s.artist_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        artist_country,
        ADD_MONTHS(release_date, 3) AS release_date_plus_3m,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM with_parent
    WHERE UPPER(TRIM(title)) LIKE 'C%'
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    artist_country,
    release_date_plus_3m,
    DATE_ADD(release_date_plus_3m, 7) AS release_date_plus_3m_plus_7d
FROM enriched
WHERE HOUR(release_date_month) BETWEEN 9 AND 14
ORDER BY title DESC NULLS FIRST, release_date_plus_3m, id
LIMIT 10
