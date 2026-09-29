WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label_2
    FROM tracks
    WHERE bpm >= 76
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.release_date_label,
        a.debut_year AS artist_debut_year
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        artist_debut_year,
        COUNT(*) OVER w_artist_id_release_date_month AS rows_in_artist_id_release_date_month,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_4,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id) AS bpm_change,
        MIN(duration_sec) OVER w_artist_id_release_date_month AS artist_id_release_date_month_min_duration_sec,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_artist_debut_ye, 3) AS artist_debut_year_percent_rank,
        FIRST_VALUE(release_date) OVER (PARTITION BY artist_id ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY bpm), 3) AS bpm_cume_dist,
        LEAD(release_date) OVER (PARTITION BY artist_id ORDER BY bpm, id) AS next_release_date,
        MIN(artist_debut_year) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)) AS artist_id_release_date_month_min_artist_debut_ye,
        FIRST_VALUE(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year,
        NTILE(5) OVER (PARTITION BY artist_id ORDER BY bpm, id) AS bpm_quintile
    FROM enriched
    WINDOW w_artist_id_release_date_month AS (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)), w_artist_id_by_artist_debut_ye AS (PARTITION BY artist_id ORDER BY artist_debut_year, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        artist_debut_year,
        rows_in_artist_id_release_date_month,
        duration_sec_avg_4,
        bpm_change,
        artist_id_release_date_month_min_duration_sec,
        artist_debut_year_percent_rank,
        first_release_date,
        next_release_date,
        artist_id_release_date_month_min_artist_debut_ye,
        MONTH(first_release_date) AS first_release_date_month
    FROM windowed
)
SELECT
    id,
    artist_id,
    release_date,
    artist_debut_year,
    rows_in_artist_id_release_date_month,
    duration_sec_avg_4,
    artist_id_release_date_month_min_duration_sec,
    first_release_date,
    next_release_date,
    first_release_date_month
FROM derived
ORDER BY id DESC NULLS FIRST
