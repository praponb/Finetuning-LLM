WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec <= 352
      AND bpm < 147
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        NTILE(4) OVER (ORDER BY duration_sec, id) AS duration_sec_quartile,
        ROUND(PERCENT_RANK() OVER w_all_by_duration_sec_ties, 3) AS duration_sec_percent_rank,
        LEAD(duration_sec) OVER w_all_by_duration_sec AS next_duration_sec,
        MAX(duration_sec) OVER (ORDER BY bpm DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_max_2_prior,
        LAG(bpm, 1, 0) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id) AS prev_bpm,
        MIN(bpm) OVER w_artist_id AS artist_id_min_bpm,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_2,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        bpm - LAG(bpm) OVER (ORDER BY duration_sec, id) AS bpm_change,
        MIN(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_min_duration_sec,
        LEAD(bpm, 3, 0) OVER (ORDER BY duration_sec, id) AS next3_bpm,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        RANK() OVER w_all_by_duration_sec_ties AS duration_sec_rank
    FROM scoped_tracks
    WINDOW w_all_by_duration_sec_ties AS (ORDER BY duration_sec), w_all_by_duration_sec AS (ORDER BY duration_sec, id), w_artist_id AS (PARTITION BY artist_id)
),
totals AS (
    -- Summarise per group
    SELECT
        duration_sec_rank,
        CASE WHEN bpm >= 147 THEN 'A' WHEN bpm >= 96 THEN 'B' ELSE 'C' END AS bpm_tier,
        duration_sec_quartile,
        SUM(artist_id_max_duration_sec) FILTER (WHERE LENGTH(TRIM(title)) >= 6) AS artist_id_max_duration_sec_length_trim_title_6,
        ROUND(AVG(running_bpm), 2) AS avg_running_bpm,
        MAX(artist_id_min_bpm) AS max_artist_id_min_bpm,
        SUM(duration_sec_max_2_prior) AS total_duration_sec_max_2_prior,
        COUNT(*) AS row_count
    FROM with_windows
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    duration_sec_rank,
    bpm_tier,
    artist_id_max_duration_sec_length_trim_title_6,
    avg_running_bpm,
    max_artist_id_min_bpm,
    total_duration_sec_max_2_prior
FROM totals
ORDER BY bpm_tier
