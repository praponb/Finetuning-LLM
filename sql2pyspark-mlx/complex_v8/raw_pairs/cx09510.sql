WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        duration_sec - bpm AS duration_sec_minus_bpm
    FROM tracks
    WHERE bpm <= 166
      AND LOWER(title) LIKE '%ur%'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, YEAR(release_date_week) ORDER BY duration_sec_minus_bpm DESC, id), 3) AS duration_sec_minus_bpm_percent_rank,
        MIN(least_bpm_duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)) AS release_date_month_min_least_bpm_duration_sec
    FROM filtered
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY is_explicit ORDER BY release_date_week NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROUND(least_bpm_duration_sec / NULLIF(SUM(least_bpm_duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS least_bpm_duration_sec_share_of_artist_id,
        MAX(least_bpm_duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_least_bpm_duration_sec
    FROM metrics
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.moods,
        s.least_bpm_duration_sec,
        s.duration_sec_minus_bpm,
        s.duration_sec_minus_bpm_percent_rank,
        s.release_date_month_min_least_bpm_duration_sec,
        s.bpm_vs_artist_id_avg,
        s.second_release_date,
        s.least_bpm_duration_sec_share_of_artist_id,
        s.artist_id_max_least_bpm_duration_sec,
        a.artist_name,
        a.country AS artist_country
    FROM metrics_2 s
    JOIN artists a ON a.id = s.artist_id
)
SELECT
    artist_id,
    COALESCE(artist_country, 'ALL') AS artist_country,
    MAX(second_release_date) AS last_second_release_date,
    MIN(release_date) AS first_release_date
FROM with_parent
GROUP BY ROLLUP(artist_id, artist_country)
ORDER BY artist_id NULLS LAST, artist_country NULLS LAST
