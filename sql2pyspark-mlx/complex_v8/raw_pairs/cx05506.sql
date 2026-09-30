WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'party')
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        moods,
        duration_sec_filled,
        LAG(duration_sec, 2) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id) AS prev2_duration_sec,
        ROW_NUMBER() OVER w_artist_id_is_explicit_by_dur AS duration_sec_filled_seq,
        MAX(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_max_3_prior,
        SUM(duration_sec) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_sum_duration_sec,
        RANK() OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id) AS bpm_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY duration_sec_filled ASC), 3) AS duration_sec_filled_cume_dist,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit, artist_id), 0), 4) AS bpm_share_of_is_explicit_artist_id,
        SUM(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_sum_3,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec_filled ASC, id) AS bpm_change,
        ROUND(duration_sec_filled - AVG(duration_sec_filled) OVER (PARTITION BY is_explicit, artist_id), 2) AS duration_sec_filled_vs_is_explicit_artist_id_avg,
        LAST_VALUE(duration_sec) OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        FIRST_VALUE(duration_sec_filled) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec_filled,
        MIN(duration_sec) OVER w_artist_id AS artist_id_min_duration_sec
    FROM recent
    WINDOW w_artist_id_is_explicit_by_dur AS (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_filled DESC, id), w_artist_id AS (PARTITION BY artist_id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(bpm_change / 50) * 50 AS bpm_change_bucket,
        duration_sec_filled - bpm_rank AS duration_sec_filled_minus_bpm_rank
    FROM with_trends
),
stats AS (
    -- Group-level totals
    SELECT
        bpm_rank,
        duration_sec_filled_seq,
        ROUND(STDDEV(duration_sec_sum_3), 2) AS stddev_duration_sec_sum_3,
        COUNT(*) AS n_rows
    FROM enriched
    GROUP BY bpm_rank, duration_sec_filled_seq
),
windowed AS (
    -- Previous / next values per key
    SELECT
        bpm_rank,
        duration_sec_filled_seq,
        stddev_duration_sec_sum_3,
        n_rows,
        ROUND(AVG(bpm_rank) OVER (PARTITION BY duration_sec_filled_seq, bpm_rank), 2) AS duration_sec_filled_seq_bpm_rank_avg_bpm_rank,
        ROUND(AVG(stddev_duration_sec_sum_3) OVER w_duration_sec_filled_seq_bpm, 2) AS duration_sec_filled_seq_bpm_rank_avg_stddev_dura,
        ROUND(CUME_DIST() OVER w_bpm_rank_n_rows_by_stddev_du, 3) AS stddev_duration_sec_sum_3_cume_dist
    FROM stats
    WINDOW w_duration_sec_filled_seq_bpm AS (PARTITION BY duration_sec_filled_seq, bpm_rank), w_bpm_rank_n_rows_by_stddev_du AS (PARTITION BY bpm_rank, n_rows ORDER BY stddev_duration_sec_sum_3, bpm_rank, duration_sec_filled_seq)
)
SELECT
    bpm_rank,
    duration_sec_filled_seq,
    stddev_duration_sec_sum_3,
    n_rows,
    duration_sec_filled_seq_bpm_rank_avg_bpm_rank,
    stddev_duration_sec_sum_3_cume_dist,
    FLOOR(duration_sec_filled_seq / 2) * 2 AS duration_sec_filled_seq_bucket,
    LEAST(n_rows, duration_sec_filled_seq) AS least_n_rows_duration_sec_filled_seq
FROM windowed
WHERE stddev_duration_sec_sum_3 >= 252.0
ORDER BY duration_sec_filled_seq_bpm_rank_avg_bpm_rank, bpm_rank NULLS LAST
