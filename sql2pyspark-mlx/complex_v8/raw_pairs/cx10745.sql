WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE (artist_id IS NOT NULL OR is_explicit = TRUE)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        SUM(duration_sec) OVER (PARTITION BY artist_id, MONTH(release_date_quarter) ORDER BY release_date_quarter ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        LAG(release_date_quarter) OVER (PARTITION BY artist_id, YEAR(release_date_quarter) ORDER BY bpm DESC, id) AS prev_release_date_quarter,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_quarter) ORDER BY bpm, id) AS bpm_quartile,
        LAST_VALUE(release_date_quarter) OVER (PARTITION BY artist_id, YEAR(release_date_quarter) ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date_quarter,
        NTILE(3) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS bpm_tercile,
        MAX(duration_sec) OVER w_artist_id AS artist_id_max_duration_sec,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_quarter) ORDER BY bpm, id) AS bpm_rank
    FROM cleaned_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id)
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    bpm,
    release_date_quarter,
    release_date_label,
    bpm_share_of_artist_id,
    rows_in_artist_id,
    running_duration_sec,
    prev_release_date_quarter,
    last_release_date_quarter,
    bpm_tercile,
    artist_id_max_duration_sec,
    bpm_rank,
    SUBSTRING(TRIM(title), 1, 4) AS title_prefix
FROM windowed
WHERE artist_id IS NOT NULL
ORDER BY title NULLS LAST
