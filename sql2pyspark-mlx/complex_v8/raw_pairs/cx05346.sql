WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_is_explicit_artist_id AS rows_in_is_explicit_artist_id,
        DENSE_RANK() OVER (ORDER BY duration_sec) AS duration_sec_dense_rank,
        bpm - LAG(bpm) OVER w_all_by_duration_sec AS bpm_change,
        SUM(bpm) OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS bpm_sum_4,
        ROW_NUMBER() OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec, id) AS seq_no,
        LAST_VALUE(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec
    FROM base_tracks
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id), w_all_by_duration_sec AS (ORDER BY duration_sec, id)
)
SELECT id, measure, amount
FROM (
    SELECT id, duration_sec, last_duration_sec
    FROM windowed
) src
UNPIVOT (
    amount FOR measure IN (duration_sec, last_duration_sec)
) unp
