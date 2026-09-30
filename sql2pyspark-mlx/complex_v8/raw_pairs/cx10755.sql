WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref,
        LOWER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE artist_id >= 8
      AND UPPER(TRIM(title)) LIKE 'S%'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country
    FROM cleaned_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_country, artist_is_verified ORDER BY duration_sec ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_3,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_country, artist_is_verified ORDER BY duration_sec ASC, id), 3) AS duration_sec_percent_rank,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY artist_is_verified)) AS distinct_artist_country_in_artist_is_verified,
        SUM(duration_sec) OVER (PARTITION BY artist_is_verified) AS artist_is_verified_sum_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_is_verified), 2) AS duration_sec_vs_artist_is_verified_avg,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_is_verified)) AS distinct_artist_id_in_artist_is_verified,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_artist_is_verified, 0), 4) AS bpm_share_of_artist_is_verified,
        COUNT(*) OVER (PARTITION BY artist_is_verified) AS rows_in_artist_is_verified
    FROM enriched
    WINDOW w_artist_is_verified AS (PARTITION BY artist_is_verified)
)
SELECT
    id,
    release_date,
    bpm,
    artist_country,
    duration_sec_avg_3,
    distinct_artist_country_in_artist_is_verified,
    duration_sec_vs_artist_is_verified_avg,
    first_known_bpm,
    bpm_share_of_artist_is_verified,
    rows_in_artist_is_verified,
    SUBSTRING(TRIM(artist_country), 1, 2) AS artist_country_prefix,
    MONTH(release_date) AS release_date_month
FROM sequenced
ORDER BY duration_sec_vs_artist_is_verified_avg DESC, duration_sec_avg_3
