WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm
    FROM tracks
    WHERE LOWER(title) LIKE '%ur%'
      AND release_date >= DATE '2024-10-23'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        COUNT(*) OVER (PARTITION BY artist_id, YEAR(release_date)) AS rows_in_artist_id_release_date_year,
        ROW_NUMBER() OVER w_artist_id_by_bpm AS rn,
        RANK() OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY bpm NULLS LAST) AS bpm_rank,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, YEAR(release_date)), 2) AS artist_id_release_date_year_avg_bpm
    FROM base_tracks
    WINDOW w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm NULLS LAST, id)
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        rows_in_artist_id_release_date_year,
        rn,
        bpm_rank,
        artist_id_release_date_year_avg_bpm
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY rows_in_artist_id_release_date_year DESC, id DESC) AS recency_rank
        FROM with_windows
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    bpm,
    rows_in_artist_id_release_date_year,
    rn,
    bpm_rank,
    QUARTER(release_date) AS release_date_quarter,
    ROUND(rn / NULLIF(bpm_rank, 0), 3) AS rn_per_bpm_rank
FROM latest_per_key
ORDER BY rows_in_artist_id_release_date_year DESC, rn_per_bpm_rank DESC
