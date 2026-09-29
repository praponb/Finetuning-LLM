WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        INITCAP(TRIM(title)) AS title_clean,
        TRUNC(release_date, 'MM') AS release_date_month_start
    FROM tracks
    WHERE YEAR(release_date) = 2024
      AND bpm BETWEEN 76 AND 166
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        title_clean,
        release_date_month_start,
        ROUND(duration_sec_per_bpm - AVG(duration_sec_per_bpm) OVER (PARTITION BY is_explicit, artist_id), 2) AS duration_sec_per_bpm_vs_is_explicit_artist_id_av,
        SUM(duration_sec) OVER (ORDER BY duration_sec_per_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROUND(AVG(duration_sec_per_bpm) OVER w_is_explicit_artist_id, 2) AS is_explicit_artist_id_avg_duration_sec_per_bpm
    FROM scoped_tracks
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
artist_id_stats AS (
    -- Median is_explicit_artist_id_avg_duration_sec_per_bpm per artist_id
    SELECT artist_id, PERCENTILE_APPROX(is_explicit_artist_id_avg_duration_sec_per_bpm, 0.5) AS median_is_explicit_artist_id_avg_duration_sec_pe
    FROM with_trends
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_is_explicit_artist_id_avg_duration_sec_pe,
        ROUND(s.is_explicit_artist_id_avg_duration_sec_per_bpm - g.median_is_explicit_artist_id_avg_duration_sec_pe, 2) AS is_explicit_artist_id_avg_duration_sec_per_bpm_m
    FROM with_trends s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.is_explicit_artist_id_avg_duration_sec_per_bpm >= g.median_is_explicit_artist_id_avg_duration_sec_pe
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    is_explicit,
    release_date_month_start,
    duration_sec_per_bpm_vs_is_explicit_artist_id_av,
    running_duration_sec,
    is_explicit_artist_id_avg_duration_sec_per_bpm,
    median_is_explicit_artist_id_avg_duration_sec_pe,
    is_explicit_artist_id_avg_duration_sec_per_bpm_m,
    DATE_TRUNC('WEEK', release_date_month_start) AS release_date_month_start_week,
    LENGTH(TRIM(title_clean)) AS title_clean_len
FROM outliers
