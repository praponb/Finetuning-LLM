WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-04-29' AND DATE '2025-03-29'
      AND duration_sec >= 209
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit, MONTH(release_date)), 0), 4) AS bpm_share_of_is_explicit_release_date_month,
        LAG(bpm, 3) OVER (ORDER BY duration_sec, id) AS prev3_bpm,
        MAX(bpm) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_max_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id), 3) AS duration_sec_percent_rank,
        RANK() OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY duration_sec DESC, id) AS duration_sec_rank,
        ROUND(CUME_DIST() OVER w_release_date_month_artist_id_3, 3) AS duration_sec_cume_dist,
        LAST_VALUE(bpm) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec), 3) AS duration_sec_cume_dist_2
    FROM valid_tracks
    WINDOW w_release_date_month_artist_id_3 AS (PARTITION BY MONTH(release_date), artist_id ORDER BY duration_sec DESC)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        bpm_max_6_prior,
        duration_sec_rank,
        last_bpm,
        duration_sec_cume_dist_2,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec_cume_dist), 3) AS duration_sec_cume_dist_cume_dist,
        ROW_NUMBER() OVER w_all_by_duration_sec_cume_dis_2 AS duration_sec_cume_dist_seq,
        MAX(duration_sec_cume_dist) OVER w_duration_sec_rank_artist_id AS duration_sec_rank_artist_id_max_duration_sec_cum,
        prev3_bpm - LAG(prev3_bpm) OVER (PARTITION BY artist_id, duration_sec_rank ORDER BY duration_sec_cume_dist_2 DESC, id) AS prev3_bpm_change
    FROM metrics
    WINDOW w_all_by_duration_sec_cume_dis_2 AS (ORDER BY duration_sec_cume_dist, id), w_duration_sec_rank_artist_id AS (PARTITION BY duration_sec_rank, artist_id)
)
-- Final projection
SELECT
    id,
    artist_id,
    last_bpm,
    duration_sec_cume_dist_cume_dist,
    duration_sec_cume_dist_seq,
    prev3_bpm_change,
    LEAST(duration_sec_cume_dist_2, last_bpm) AS least_duration_sec_cume_dist_2_last_bpm,
    LEAST(prev3_bpm_change, duration_sec_cume_dist_2) AS least_prev3_bpm_change_duration_sec_cume_dist_2
FROM ranked
ORDER BY last_bpm DESC
