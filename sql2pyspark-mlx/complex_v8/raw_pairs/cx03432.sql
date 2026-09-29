WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE release_date < DATE '2025-03-08'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        LEAD(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id) AS next_duration_sec,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        DENSE_RANK() OVER (PARTITION BY MONTH(release_date) ORDER BY release_date DESC, id) AS release_date_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id) AS bpm_seq,
        SUM(bpm) OVER (PARTITION BY artist_id, MONTH(release_date) ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY MONTH(release_date) ORDER BY release_date DESC, id) AS duration_sec_change,
        ROW_NUMBER() OVER (PARTITION BY artist_id, MONTH(release_date) ORDER BY bpm ASC, id) AS seq_no,
        LAG(release_date) OVER w_artist_id_release_date_month_2 AS prev_release_date,
        RANK() OVER w_is_explicit_artist_id_by_bpm_4 AS bpm_rank,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit) AS rows_in_artist_id_is_explicit,
        NTILE(5) OVER (PARTITION BY MONTH(release_date) ORDER BY release_date DESC, id) AS release_date_quintile
    FROM scoped_tracks
    WINDOW w_artist_id_release_date_month_2 AS (PARTITION BY artist_id, MONTH(release_date) ORDER BY bpm ASC, id), w_is_explicit_artist_id_by_bpm_4 AS (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY seq_no, bpm_seq ORDER BY second_duration_sec ASC, id), 3) AS second_duration_sec_cume_dist,
        ROUND(bpm_seq - AVG(bpm_seq) OVER (PARTITION BY MONTH(prev_release_date)), 2) AS bpm_seq_vs_prev_release_date_month_avg,
        LEAD(duration_sec_change, 3) OVER w_seq_no_by_prev_release_date AS next3_duration_sec_change,
        RANK() OVER w_seq_no_bpm_seq_by_bpm AS bpm_rank_2,
        ROUND(AVG(release_date_quintile) OVER (PARTITION BY seq_no, bpm_seq ORDER BY bpm DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS release_date_quintile_avg_4
    FROM with_trends
    WINDOW w_seq_no_by_prev_release_date AS (PARTITION BY seq_no ORDER BY prev_release_date DESC, id), w_seq_no_bpm_seq_by_bpm AS (PARTITION BY seq_no, bpm_seq ORDER BY bpm DESC, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    bpm,
    last_known_bpm,
    second_duration_sec,
    next_duration_sec,
    first_known_release_date,
    bpm_seq,
    running_bpm,
    duration_sec_change,
    seq_no,
    prev_release_date,
    bpm_rank,
    rows_in_is_explicit,
    release_date_quintile,
    bpm_seq_vs_prev_release_date_month_avg,
    next3_duration_sec_change,
    bpm_rank_2,
    release_date_quintile_avg_4,
    CONCAT(title, '#', CAST(rows_in_is_explicit AS STRING)) AS title_ref
FROM ranked
ORDER BY release_date, bpm_seq_vs_prev_release_date_month_avg DESC
