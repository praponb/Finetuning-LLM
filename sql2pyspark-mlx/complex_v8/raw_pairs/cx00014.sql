WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods
    FROM tracks
    WHERE duration_sec > 209
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        MIN(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        MAX(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS duration_sec_max_1_prior,
        LAG(duration_sec, 2) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id) AS prev2_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        LAG(duration_sec) OVER (ORDER BY duration_sec ASC NULLS LAST, id) AS prev_duration_sec,
        LEAD(duration_sec, 3) OVER (PARTITION BY artist_id ORDER BY duration_sec, id) AS next3_duration_sec,
        ROW_NUMBER() OVER w_all_by_duration_sec AS seq_no,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id, 0), 4) AS duration_sec_share_of_artist_id,
        NTILE(10) OVER (ORDER BY duration_sec ASC NULLS LAST, id) AS duration_sec_decile,
        SUM(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        DENSE_RANK() OVER (ORDER BY duration_sec ASC NULLS LAST) AS duration_sec_dense_rank
    FROM valid_tracks
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec ASC NULLS LAST, id), w_artist_id AS (PARTITION BY artist_id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE duration_sec_dense_rank = 1
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    moods,
    min_duration_sec_so_far,
    prev2_duration_sec,
    artist_id_max_duration_sec,
    prev_duration_sec,
    seq_no,
    duration_sec_decile,
    running_duration_sec,
    last_known_duration_sec,
    duration_sec_dense_rank
FROM first_rows
WHERE duration_sec_share_of_artist_id <= 0.7
ORDER BY duration_sec_dense_rank, id
LIMIT 20
