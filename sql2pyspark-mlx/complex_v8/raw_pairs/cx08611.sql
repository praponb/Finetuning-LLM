WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        moods,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE is_explicit = TRUE
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.moods,
        s.moods_text,
        a.is_verified AS artist_is_verified
    FROM filtered s
    JOIN artists a ON a.id = s.artist_id AND a.country IN ('JP', 'TH')
    WHERE a.is_verified = FALSE
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY artist_is_verified ORDER BY duration_sec, id) AS duration_sec_decile,
        COUNT(*) OVER w_artist_is_verified_release_d AS rows_in_artist_is_verified_release_date_year,
        MAX(duration_sec) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY duration_sec ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far,
        MAX(duration_sec) OVER w_artist_is_verified_release_d AS artist_is_verified_release_date_year_max_duratio,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_is_verified, YEAR(release_date))) AS distinct_artist_id_in_artist_is_verified_release,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id, artist_is_verified ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, artist_is_verified ORDER BY release_date DESC), 3) AS release_date_percent_rank,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_is_verified, YEAR(release_date)), 2) AS artist_is_verified_release_date_year_avg_duratio
    FROM enriched_artist
    WINDOW w_artist_is_verified_release_d AS (PARTITION BY artist_is_verified, YEAR(release_date))
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ABS(first_known_duration_sec - 184) AS first_known_duration_sec_distance,
        LOWER(REGEXP_REPLACE(TRIM(moods_text), ' ', '_')) AS moods_text_slug,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM windowed
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    moods,
    moods_text,
    artist_is_verified,
    duration_sec_decile,
    rows_in_artist_is_verified_release_date_year,
    max_duration_sec_so_far,
    artist_is_verified_release_date_year_max_duratio,
    distinct_artist_id_in_artist_is_verified_release,
    first_known_duration_sec,
    release_date_percent_rank,
    artist_is_verified_release_date_year_avg_duratio,
    first_known_duration_sec_distance,
    moods_text_slug,
    release_date_month,
    DATE_FORMAT(release_date_month, 'MMM yyyy') AS release_date_month_label
FROM enriched
ORDER BY release_date_month_label DESC
