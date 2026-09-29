WITH base AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE is_explicit = TRUE
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_is_explicit_artist_id_by_dur AS duration_sec_rank,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit, artist_id), 2) AS is_explicit_artist_id_avg_duration_sec,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit, artist_id), 2) AS is_explicit_artist_id_avg_bpm,
        COUNT(*) OVER w_is_explicit_artist_id AS rows_in_is_explicit_artist_id,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm), 3) AS bpm_cume_dist,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_is_explicit_artist_id, 0), 4) AS bpm_share_of_is_explicit_artist_id,
        SUM(duration_sec) OVER w_is_explicit_artist_id AS is_explicit_artist_id_sum_duration_sec,
        SUM(duration_sec) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_sum_3,
        LAG(bpm) OVER (ORDER BY release_date ASC, id) AS prev_bpm,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        RANK() OVER (ORDER BY release_date DESC) AS release_date_rank
    FROM base
    WINDOW w_is_explicit_artist_id_by_dur AS (PARTITION BY is_explicit, artist_id ORDER BY duration_sec DESC NULLS FIRST, id), w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        is_explicit_artist_id_avg_duration_sec,
        is_explicit_artist_id_avg_bpm,
        rows_in_is_explicit_artist_id,
        bpm_cume_dist,
        bpm_share_of_is_explicit_artist_id,
        is_explicit_artist_id_sum_duration_sec,
        duration_sec_sum_3,
        prev_bpm,
        second_bpm,
        release_date_rank
    FROM windowed
    WHERE duration_sec_rank = 1
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    bpm,
    moods,
    is_explicit_artist_id_avg_duration_sec,
    rows_in_is_explicit_artist_id,
    bpm_cume_dist,
    bpm_share_of_is_explicit_artist_id,
    is_explicit_artist_id_sum_duration_sec,
    second_bpm,
    release_date_rank
FROM first_rows
ORDER BY second_bpm, bpm_cume_dist
