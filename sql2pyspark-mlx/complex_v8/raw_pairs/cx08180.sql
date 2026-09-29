WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN bpm >= 156 THEN 'A' WHEN bpm >= 94 THEN 'B' ELSE 'C' END AS bpm_tier
    FROM tracks
    WHERE bpm >= 94
      AND artist_id <= 3
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY bpm_tier, is_explicit ORDER BY bpm ASC, id)) AS days_since_prev_release_date,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit), 0), 4) AS duration_sec_share_of_is_explicit,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id, bpm_tier), 2) AS duration_sec_vs_artist_id_bpm_tier_avg,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec), 3) AS duration_sec_percent_rank,
        COUNT(*) OVER w_bpm_tier AS rows_in_bpm_tier,
        ROUND(AVG(duration_sec) OVER (PARTITION BY bpm_tier, artist_id ORDER BY release_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_7,
        COUNT(*) OVER w_artist_id_bpm_tier AS rows_in_artist_id_bpm_tier,
        MIN(bpm) OVER (PARTITION BY bpm_tier ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_so_far,
        RANK() OVER (PARTITION BY bpm_tier, artist_id ORDER BY release_date DESC) AS release_date_rank,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY bpm_tier ORDER BY duration_sec DESC, id)) AS days_since_prev_release_date_2,
        LAG(release_date) OVER (PARTITION BY bpm_tier ORDER BY duration_sec DESC, id) AS prev_release_date,
        RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec DESC) AS duration_sec_rank,
        MIN(duration_sec) OVER (PARTITION BY artist_id, bpm_tier) AS artist_id_bpm_tier_min_duration_sec
    FROM scoped_tracks
    WINDOW w_bpm_tier AS (PARTITION BY bpm_tier), w_artist_id_bpm_tier AS (PARTITION BY artist_id, bpm_tier)
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    bpm_avg_3_prior,
    duration_sec_share_of_is_explicit,
    duration_sec_vs_artist_id_bpm_tier_avg,
    rows_in_is_explicit,
    duration_sec_percent_rank,
    rows_in_artist_id_bpm_tier,
    min_bpm_so_far,
    days_since_prev_release_date_2,
    duration_sec_rank,
    artist_id_bpm_tier_min_duration_sec,
    ABS(bpm - 143) AS bpm_distance,
    ABS(rows_in_is_explicit - 9) AS rows_in_is_explicit_distance
FROM ranked
WHERE rows_in_is_explicit BETWEEN 5 AND 18
ORDER BY duration_sec_share_of_is_explicit DESC, bpm, id
LIMIT 20
