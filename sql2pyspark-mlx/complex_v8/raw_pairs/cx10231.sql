WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        COALESCE(bpm, 0) AS bpm_filled,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE bpm BETWEEN 75 AND 163
      AND UPPER(TRIM(title)) LIKE 'C%'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.bpm_filled,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM cleaned_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id AND LENGTH(TRIM(a.artist_name)) >= 7
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(bpm) OVER (PARTITION BY MONTH(release_date)) AS release_date_month_min_bpm,
        SUM(duration_sec) OVER (PARTITION BY MONTH(release_date)) AS release_date_month_sum_duration_sec,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY MONTH(release_date)), 0), 4) AS bpm_share_of_release_date_month,
        MAX(duration_sec) OVER (PARTITION BY MONTH(release_date)) AS release_date_month_max_duration_sec,
        RANK() OVER (PARTITION BY artist_id ORDER BY release_date DESC) AS release_date_rank,
        ROUND(AVG(bpm) OVER (PARTITION BY MONTH(release_date)), 2) AS release_date_month_avg_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_4,
        COUNT(*) OVER (PARTITION BY MONTH(release_date)) AS rows_in_release_date_month,
        LEAD(artist_debut_year) OVER w_artist_id_by_release_date AS next_artist_debut_year,
        ROW_NUMBER() OVER (PARTITION BY MONTH(release_date) ORDER BY bpm DESC, id) AS seq_no,
        duration_sec - LAG(duration_sec) OVER w_artist_id_by_release_date AS duration_sec_change,
        SUM(bpm) OVER (PARTITION BY MONTH(release_date)) AS release_date_month_sum_bpm,
        NTILE(10) OVER (PARTITION BY artist_id, artist_country ORDER BY release_date DESC, id) AS release_date_decile
    FROM with_parent
    WINDOW w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date DESC, id), w_release_date_month AS (PARTITION BY MONTH(release_date))
),
enriched AS (
    -- Derive helper columns
    SELECT *, SIZE(moods) AS moods_count
    FROM windowed
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    moods,
    bpm_filled,
    artist_country,
    artist_debut_year,
    release_date_month_min_bpm,
    bpm_share_of_release_date_month,
    release_date_month_max_duration_sec,
    bpm_avg_4,
    rows_in_release_date_month,
    next_artist_debut_year,
    duration_sec_change,
    release_date_month_sum_bpm,
    release_date_decile,
    YEAR(release_date) AS release_date_year,
    CASE WHEN moods_count >= 2 THEN 'HIGH' WHEN moods_count >= 1 THEN 'MEDIUM' ELSE 'LOW' END AS moods_count_tier
FROM enriched
ORDER BY release_date_month_sum_bpm NULLS LAST, duration_sec, id
LIMIT 20
