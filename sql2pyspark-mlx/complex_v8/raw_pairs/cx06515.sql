WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm
    FROM tracks
    WHERE release_date < DATE '2025-03-08'
      AND is_explicit = FALSE
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(greatest_duration_sec_bpm / NULLIF(SUM(greatest_duration_sec_bpm) OVER (PARTITION BY artist_id), 0), 4) AS greatest_duration_sec_bpm_share_of_artist_id,
        ROUND(AVG(bpm_bucket) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_bpm_bucket,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id) AS duration_sec_dense_rank,
        NTH_VALUE(bpm_bucket, 2) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_bucket,
        RANK() OVER w_all_by_duration_sec AS duration_sec_rank,
        MIN(duration_sec) OVER w_is_explicit_artist_id AS is_explicit_artist_id_min_duration_sec,
        RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_rank_2,
        LEAD(duration_sec, 1, 0) OVER (ORDER BY duration_sec, id) AS next_duration_sec,
        MAX(bpm_bucket) OVER (PARTITION BY artist_id) AS artist_id_max_bpm_bucket,
        duration_sec - LAG(duration_sec) OVER w_all_by_duration_sec AS duration_sec_change,
        LAG(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id) AS prev_duration_sec,
        DENSE_RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm_bucket, id) AS bpm_bucket_dense_rank
    FROM valid
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec, id), w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY is_explicit, bpm_bucket_dense_rank ORDER BY is_explicit_artist_id_min_duration_sec NULLS LAST, id) AS is_explicit_artist_id_min_duration_sec_dense_ran,
        MAX(duration_sec_dense_rank) OVER (PARTITION BY duration_sec_dense_rank ORDER BY artist_id_avg_bpm_bucket DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_dense_rank_max_2_prior,
        MAX(duration_sec) OVER w_artist_id_is_explicit AS artist_id_is_explicit_max_duration_sec
    FROM with_windows
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
)
SELECT
    id,
    title,
    greatest_duration_sec_bpm_share_of_artist_id,
    artist_id_avg_bpm_bucket,
    duration_sec_dense_rank,
    second_bpm_bucket,
    duration_sec_rank_2,
    artist_id_max_bpm_bucket,
    prev_duration_sec,
    is_explicit_artist_id_min_duration_sec_dense_ran,
    duration_sec_dense_rank_max_2_prior,
    artist_id_is_explicit_max_duration_sec
FROM ranked
ORDER BY artist_id_is_explicit_max_duration_sec NULLS LAST, id
