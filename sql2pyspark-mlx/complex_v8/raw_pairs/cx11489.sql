WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        INITCAP(TRIM(title)) AS title_clean
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'N%'
      AND duration_sec > 165
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        ROUND(AVG(bpm), 2) AS avg_track_bpm,
        MIN(release_date) AS first_track_release_date,
        COUNT(*) AS track_count
    FROM tracks
    GROUP BY artist_id
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.avg_track_bpm,
        t.first_track_release_date,
        COALESCE(t.track_count, 0) AS track_count
    FROM base_tracks s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.release_date_week,
        s.title_clean,
        s.first_track_release_date,
        s.track_count,
        a.genre AS artist_genre
    FROM with_stats s
    LEFT JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_duration_sec AS duration_sec_dense_rank,
        COUNT(*) OVER (PARTITION BY YEAR(release_date), artist_id) AS rows_in_release_date_year_artist_id,
        ROUND(AVG(duration_sec) OVER (PARTITION BY YEAR(release_date), artist_id), 2) AS release_date_year_artist_id_avg_duration_sec,
        SIZE(COLLECT_SET(artist_genre) OVER (PARTITION BY YEAR(release_date), artist_id)) AS distinct_artist_genre_in_release_date_year_artis,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_week) ORDER BY bpm ASC, id) AS bpm_seq
    FROM with_details_artist
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, bpm_seq, duration_sec_dense_rank
    FROM sequenced
) src
UNPIVOT (
    amount FOR measure IN (bpm_seq, duration_sec_dense_rank)
) unp
