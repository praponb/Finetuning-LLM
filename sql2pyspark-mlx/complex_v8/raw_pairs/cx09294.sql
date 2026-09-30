WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        moods,
        ABS(bpm - 104) AS bpm_distance,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'R%'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        duration_sec,
        release_date,
        bpm_distance,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY bpm_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY bpm_distance DESC, id) AS duration_sec_change,
        ROUND(bpm_distance / NULLIF(SUM(bpm_distance) OVER (PARTITION BY artist_id), 0), 4) AS bpm_distance_share_of_artist_id
    FROM scoped
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY bpm_distance DESC, id) AS bpm_distance_quartile,
        ROUND(AVG(duration_sec_change) OVER (ORDER BY bpm_distance DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_change_avg_3_prior
    FROM metrics
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM with_windows s
    LEFT JOIN play_stats p ON p.track_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    play_count,
    bpm_distance_quartile,
    GROUPING_ID() AS grouping_level,
    DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days,
    ROUND(SUM(duration_sec_change_avg_3_prior) / NULLIF(SUM(duration_sec), 0), 3) AS duration_sec_change_avg_3_prior_to_duration_sec_,
    ROUND(SUM(duration_sec_change_avg_3_prior) / NULLIF(SUM(bpm_distance_share_of_artist_id), 0), 3) AS duration_sec_change_avg_3_prior_to_bpm_distance_
FROM with_stats
GROUP BY GROUPING SETS ((play_count, bpm_distance_quartile), (play_count), ())
