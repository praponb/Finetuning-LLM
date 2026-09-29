WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        ABS(bpm - 118) AS bpm_distance,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE bpm >= 96
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm_distance,
        duration_sec_bucket,
        bpm_bucket,
        COALESCE(duration_sec_bucket, 0) AS duration_sec_bucket_filled,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM prepared
    WHERE is_explicit = TRUE
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm_distance,
        s.duration_sec_bucket,
        s.bpm_bucket,
        s.duration_sec_bucket_filled,
        s.release_date_label,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM labeled s
    JOIN artists a ON a.id = s.artist_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_country,
        artist_id,
        artist_genre,
        ROUND(100 * COUNT(CASE WHEN is_explicit = TRUE THEN 1 END) / COUNT(*), 1) AS pct_is_explicit_true,
        MAX(bpm_distance) AS max_bpm_distance,
        MIN(bpm_bucket) AS min_bpm_bucket,
        SUM(CASE WHEN artist_country NOT IN ('US', 'KR') THEN 1 ELSE 0 END) AS n_artist_country_not_in_us_kr,
        ARRAY_SORT(COLLECT_SET(artist_genre)) AS artist_genre_list
    FROM joined
    GROUP BY 1, 2, 3
),
enriched AS (
    -- Add calculated columns
    SELECT *, FLOOR(pct_is_explicit_true / 25) * 25 AS pct_is_explicit_true_bucket
    FROM totals
)
-- Native SQL UNPIVOT (wide to long)
SELECT artist_country, artist_id, measure, value
FROM (
    SELECT artist_country, artist_id, n_artist_country_not_in_us_kr, max_bpm_distance
    FROM enriched
) src
UNPIVOT (
    value FOR measure IN (n_artist_country_not_in_us_kr, max_bpm_distance)
) unp
ORDER BY artist_country, artist_id, measure
