WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        LEAST(duration_sec, bpm) AS least_duration_sec_bpm,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'chill')
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        s.least_duration_sec_bpm,
        s.title_prefix,
        a.artist_name
    FROM filtered s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        bpm,
        least_duration_sec_bpm,
        title_prefix,
        artist_name,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id) AS bpm_dense_rank,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id) AS bpm_quartile
    FROM joined
)
SELECT
    id,
    artist_id,
    title,
    bpm,
    least_duration_sec_bpm,
    title_prefix,
    artist_name,
    bpm_dense_rank,
    bpm_quartile,
    FLOOR(bpm / 25) * 25 AS bpm_bucket,
    COALESCE(least_duration_sec_bpm, 0) AS least_duration_sec_bpm_filled
FROM with_windows
WHERE LOWER(title) LIKE '%ar%'
ORDER BY artist_id, bpm_bucket
