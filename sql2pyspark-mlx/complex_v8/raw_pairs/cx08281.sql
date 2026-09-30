WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-04-29' AND DATE '2025-03-29'
      AND is_explicit = TRUE
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        duration_sec,
        is_explicit,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        MAX(duration_sec) OVER w_is_explicit_artist_id AS is_explicit_artist_id_max_duration_sec,
        LAG(bpm, 2) OVER w_artist_id_is_explicit_by_dur_3 AS prev2_bpm
    FROM cleaned
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id), w_artist_id_is_explicit_by_dur_3 AS (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        second_duration_sec,
        prev2_bpm,
        LEAD(first_known_duration_sec, 1, 0) OVER (PARTITION BY is_explicit ORDER BY second_duration_sec, id) AS next_first_known_duration_sec,
        DENSE_RANK() OVER w_is_explicit_by_second_durati AS second_duration_sec_dense_rank
    FROM sequenced
    WINDOW w_is_explicit_by_second_durati AS (PARTITION BY is_explicit ORDER BY second_duration_sec, id)
)
-- Final projection
SELECT
    id,
    second_duration_sec,
    prev2_bpm,
    next_first_known_duration_sec,
    second_duration_sec_dense_rank
FROM with_windows
