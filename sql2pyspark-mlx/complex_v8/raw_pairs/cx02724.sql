WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND duration_sec >= 213
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        duration_sec,
        bpm,
        moods,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_duration_sec, 3) AS duration_sec_percent_rank,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS is_explicit_avg_bpm,
        LAG(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec ASC NULLS LAST, id) AS prev_bpm,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        ROW_NUMBER() OVER w_artist_id_by_duration_sec AS duration_sec_seq,
        ROUND(CUME_DIST() OVER w_is_explicit_artist_id_by_dur_2, 3) AS duration_sec_cume_dist
    FROM recent
    WINDOW w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec ASC NULLS LAST, id), w_is_explicit_artist_id_by_dur_2 AS (PARTITION BY is_explicit, artist_id ORDER BY duration_sec)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE duration_sec_seq <= 5
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(running_duration_sec / NULLIF(SUM(running_duration_sec) OVER w_duration_sec_seq, 0), 4) AS running_duration_sec_share_of_duration_sec_seq,
        RANK() OVER (ORDER BY duration_sec, id) AS duration_sec_rank,
        COUNT(*) OVER w_duration_sec_seq AS rows_in_duration_sec_seq,
        ROW_NUMBER() OVER (ORDER BY duration_sec, id) AS rn,
        ROUND(AVG(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_3_prior,
        LAG(duration_sec_seq) OVER (ORDER BY duration_sec, id) AS prev_duration_sec_seq,
        ROUND(PERCENT_RANK() OVER w_all_by_duration_sec_2, 3) AS duration_sec_percent_rank_2
    FROM first_rows
    WINDOW w_duration_sec_seq AS (PARTITION BY duration_sec_seq), w_all_by_duration_sec_2 AS (ORDER BY duration_sec NULLS LAST, id)
),
summary AS (
    -- Group-level totals
    SELECT
        duration_sec_rank,
        duration_sec_seq,
        rn,
        COUNT(*) AS cnt,
        ROUND(AVG(duration_sec_avg_3_prior), 2) AS avg_duration_sec_avg_3_prior,
        ROUND(STDDEV(prev_duration_sec_seq), 2) AS stddev_prev_duration_sec_seq
    FROM with_windows_2
    GROUP BY duration_sec_rank, duration_sec_seq, rn
)
SELECT
    duration_sec_rank,
    duration_sec_seq,
    rn,
    cnt,
    stddev_prev_duration_sec_seq,
    LEAST(duration_sec_rank, cnt) AS least_duration_sec_rank_cnt,
    COALESCE(duration_sec_seq, 0) AS duration_sec_seq_filled
FROM summary
ORDER BY cnt DESC
