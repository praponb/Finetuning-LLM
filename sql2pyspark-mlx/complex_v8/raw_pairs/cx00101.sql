WITH cleaned_tracks AS (
    -- Filter tracks
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
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec, id)) AS days_since_prev_release_date,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)), 2) AS artist_id_release_date_month_avg_duration_sec,
        SUM(bpm) OVER w_release_date_month AS release_date_month_sum_bpm,
        DATEDIFF(release_date, LAG(release_date) OVER w_all_by_release_date) AS days_since_prev_release_date_2,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY is_explicit ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        RANK() OVER (PARTITION BY is_explicit ORDER BY duration_sec) AS duration_sec_rank,
        ROUND(AVG(duration_sec) OVER w_release_date_month, 2) AS release_date_month_avg_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm, id), 3) AS bpm_cume_dist,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)), 0), 4) AS duration_sec_share_of_release_date_month,
        ROUND(CUME_DIST() OVER w_is_explicit_artist_id_by_dur_2, 3) AS duration_sec_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY release_date DESC, id), 3) AS release_date_percent_rank
    FROM cleaned_tracks
    WINDOW w_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', release_date)), w_all_by_release_date AS (ORDER BY release_date DESC, id), w_is_explicit_artist_id_by_dur_2 AS (PARTITION BY is_explicit, artist_id ORDER BY duration_sec)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        MAX(second_bpm) OVER (ORDER BY artist_id_release_date_month_avg_duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS second_bpm_max_4,
        SUM(release_date_percent_rank) OVER (PARTITION BY distinct_artist_id_in_is_explicit ORDER BY duration_sec_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_release_date_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY distinct_artist_id_in_is_explicit ORDER BY duration_sec_cume_dist, id) AS duration_sec_cume_dist_seq,
        ROUND(duration_sec_rank / NULLIF(SUM(duration_sec_rank) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_rank_share_of_artist_id,
        MIN(duration_sec_cume_dist) OVER w_artist_id AS artist_id_min_duration_sec_cume_dist,
        MIN(duration_sec_rank) OVER (PARTITION BY artist_id) AS artist_id_min_duration_sec_rank
    FROM with_trends
    WINDOW w_artist_id AS (PARTITION BY artist_id)
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    is_explicit,
    bpm,
    moods,
    days_since_prev_release_date,
    second_duration_sec,
    distinct_artist_id_in_is_explicit,
    artist_id_release_date_month_avg_duration_sec,
    release_date_month_sum_bpm,
    days_since_prev_release_date_2,
    duration_sec_rank,
    release_date_month_avg_duration_sec,
    duration_sec_share_of_release_date_month,
    duration_sec_cume_dist,
    release_date_percent_rank,
    second_bpm_max_4,
    duration_sec_cume_dist_seq,
    duration_sec_rank_share_of_artist_id,
    artist_id_min_duration_sec_cume_dist,
    artist_id_min_duration_sec_rank,
    CASE WHEN second_duration_sec >= 353 THEN 'A' WHEN second_duration_sec >= 184 THEN 'B' ELSE 'C' END AS second_duration_sec_tier
FROM metrics
ORDER BY id, duration_sec_share_of_release_date_month DESC NULLS FIRST
LIMIT 5
