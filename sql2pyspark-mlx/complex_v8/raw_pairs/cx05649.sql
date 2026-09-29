WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM tracks
    WHERE NOT is_explicit
),
tagged AS (
    -- Label and score rows
    SELECT *, bpm_per_duration_sec - bpm AS bpm_per_duration_sec_minus_bpm
    FROM base
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        bpm,
        release_date_year,
        COUNT(*) OVER w_is_explicit_artist_id AS rows_in_is_explicit_artist_id,
        SUM(bpm) OVER w_is_explicit_artist_id AS is_explicit_artist_id_sum_bpm
    FROM tagged
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
)
-- Final projection
SELECT
    id,
    bpm,
    release_date_year,
    rows_in_is_explicit_artist_id,
    is_explicit_artist_id_sum_bpm
FROM with_windows
ORDER BY rows_in_is_explicit_artist_id NULLS LAST
