WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE bpm > 76
      AND release_date < DATE '2025-08-02'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY is_explicit), 2) AS duration_sec_vs_is_explicit_avg,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        ROUND(CUME_DIST() OVER (ORDER BY bpm), 3) AS bpm_cume_dist,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id) AS seq_no,
        LAST_VALUE(duration_sec) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY is_explicit, artist_id), 2) AS duration_sec_vs_is_explicit_artist_id_avg,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec), 3) AS duration_sec_cume_dist,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit), 0), 4) AS bpm_share_of_is_explicit
    FROM base
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY duration_sec_vs_is_explicit_artist_id_avg DESC), 3) AS duration_sec_vs_is_explicit_artist_id_avg_cume_d,
        ROUND(duration_sec_avg_2_prior - AVG(duration_sec_avg_2_prior) OVER (PARTITION BY artist_id, seq_no), 2) AS duration_sec_avg_2_prior_vs_artist_id_seq_no_avg,
        RANK() OVER w_is_explicit_by_duration_sec AS duration_sec_vs_is_explicit_artist_id_avg_rank,
        NTH_VALUE(bpm_cume_dist, 2) OVER (PARTITION BY artist_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_cume_dist,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY seq_no) AS seq_no_dense_rank
    FROM windowed
    WINDOW w_is_explicit_by_duration_sec AS (PARTITION BY is_explicit ORDER BY duration_sec_vs_is_explicit_artist_id_avg DESC)
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    duration_sec_vs_is_explicit_avg,
    last_known_bpm,
    bpm_cume_dist,
    duration_sec_avg_2_prior,
    last_duration_sec,
    duration_sec_vs_is_explicit_artist_id_avg,
    duration_sec_cume_dist,
    bpm_share_of_is_explicit,
    duration_sec_vs_is_explicit_artist_id_avg_cume_d,
    duration_sec_vs_is_explicit_artist_id_avg_rank,
    second_bpm_cume_dist,
    seq_no_dense_rank,
    LEAST(duration_sec_cume_dist, duration_sec_avg_2_prior) AS least_duration_sec_cume_dist_duration_sec_avg_2_
FROM with_trends
ORDER BY least_duration_sec_cume_dist_duration_sec_avg_2_ DESC NULLS FIRST, seq_no_dense_rank, id
LIMIT 10
