WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tracks
    WHERE is_explicit = TRUE
      AND release_date < DATE '2025-08-07'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        MAX(duration_sec) OVER w_release_date_month_artist_id AS release_date_month_artist_id_max_duration_sec,
        NTILE(4) OVER (ORDER BY bpm ASC, id) AS bpm_quartile,
        LEAD(duration_sec, 3) OVER (ORDER BY bpm ASC, id) AS next3_duration_sec,
        RANK() OVER (ORDER BY bpm ASC, id) AS bpm_rank,
        ROUND(AVG(duration_sec) OVER (ORDER BY bpm ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_3
    FROM valid_tracks
    WINDOW w_release_date_month_artist_id AS (PARTITION BY MONTH(release_date), artist_id)
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, a.artist_name, a.genre AS artist_genre
    FROM sequenced s
    JOIN artists a ON a.id = s.artist_id
    WHERE UPPER(TRIM(a.artist_name)) LIKE 'K%'
)
SELECT id, metric, value
FROM (
    SELECT id, bpm_rank, bpm, release_date_month_artist_id_max_duration_sec, bpm_quartile
    FROM joined
) src
UNPIVOT (
    value FOR metric IN (bpm_rank, bpm, release_date_month_artist_id_max_duration_sec, bpm_quartile)
) unp
