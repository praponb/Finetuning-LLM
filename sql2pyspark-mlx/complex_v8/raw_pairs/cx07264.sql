WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(bpm, 0) AS bpm_filled,
        YEAR(release_date) AS release_date_year,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE duration_sec <= 392
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY artist_id, title_len) AS rows_in_artist_id_title_len,
        LEAD(duration_sec) OVER w_all_by_title_len AS next_duration_sec
    FROM base_tracks
    WINDOW w_all_by_title_len AS (ORDER BY title_len DESC, id)
)
-- Subtotals with GROUPING SETS
SELECT
    release_date_year,
    title_len,
    GROUPING_ID() AS gid,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM windowed
GROUP BY GROUPING SETS ((release_date_year, title_len), (release_date_year), ())
