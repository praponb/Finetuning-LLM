WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec BETWEEN 184 AND 409
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.country AS artist_country
    FROM base_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        is_explicit,
        bpm,
        moods,
        artist_country,
        SUM(bpm) OVER (PARTITION BY MONTH(release_date) ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS bpm_sum_4,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id)) AS days_since_prev_release_date,
        ROW_NUMBER() OVER w_is_explicit_by_bpm AS rn,
        NTILE(10) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id) AS bpm_decile,
        MAX(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        ROW_NUMBER() OVER (PARTITION BY MONTH(release_date) ORDER BY bpm DESC NULLS FIRST, id) AS bpm_seq,
        MAX(bpm) OVER (PARTITION BY artist_country) AS artist_country_max_bpm,
        MIN(bpm) OVER (PARTITION BY artist_country) AS artist_country_min_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_country), 2) AS artist_country_avg_bpm
    FROM with_details_artist
    WINDOW w_is_explicit_by_bpm AS (PARTITION BY is_explicit ORDER BY bpm DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        is_explicit,
        bpm,
        moods,
        artist_country,
        bpm_sum_4,
        days_since_prev_release_date,
        bpm_decile,
        max_bpm_so_far,
        bpm_seq,
        artist_country_max_bpm,
        artist_country_min_bpm,
        artist_country_avg_bpm
    FROM metrics
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    is_explicit,
    moods,
    artist_country,
    bpm_sum_4,
    days_since_prev_release_date,
    bpm_decile,
    max_bpm_so_far,
    artist_country_max_bpm,
    artist_country_min_bpm,
    artist_country_avg_bpm,
    CASE WHEN bpm_sum_4 >= 468 THEN 'A' WHEN bpm_sum_4 >= 204 THEN 'B' ELSE 'C' END AS bpm_sum_4_tier,
    GREATEST(bpm_decile, artist_country_min_bpm) AS greatest_bpm_decile_artist_country_min_bpm
FROM top_per_group
WHERE SIZE(moods) >= 1
ORDER BY bpm_sum_4 DESC NULLS FIRST, id
LIMIT 5
