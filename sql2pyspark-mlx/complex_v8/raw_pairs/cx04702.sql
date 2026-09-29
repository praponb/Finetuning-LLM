WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'workout')
      AND UPPER(TRIM(title)) LIKE 'M%'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        title,
        moods,
        days_since_release_date,
        release_date_label,
        duration_sec - LAG(duration_sec) OVER (ORDER BY bpm DESC, id) AS duration_sec_change,
        LEAD(bpm) OVER (ORDER BY bpm DESC, id) AS next_bpm,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        NTILE(5) OVER w_all_by_bpm AS bpm_quintile
    FROM valid_tracks
    WINDOW w_all_by_bpm AS (ORDER BY bpm DESC, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        days_since_release_date,
        duration_sec_change,
        next_bpm,
        duration_sec_vs_artist_id_avg,
        MIN(next_bpm) OVER (PARTITION BY bpm_quintile) AS bpm_quintile_min_next_bpm,
        SUM(bpm_quintile) OVER (PARTITION BY bpm_quintile ORDER BY duration_sec_change DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm_quintile,
        SUM(duration_sec_change) OVER (PARTITION BY bpm_quintile ORDER BY duration_sec_change DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_change,
        NTH_VALUE(duration_sec_change, 2) OVER (PARTITION BY bpm_quintile ORDER BY duration_sec_change DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_change
    FROM ranked
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        days_since_release_date,
        duration_sec_change,
        next_bpm,
        bpm_quintile_min_next_bpm,
        running_bpm_quintile,
        running_duration_sec_change,
        second_duration_sec_change,
        running_bpm_quintile - days_since_release_date AS running_bpm_quintile_minus_days_since_release_da,
        ROUND(duration_sec_change / NULLIF(days_since_release_date, 0), 3) AS duration_sec_change_per_days_since_release_date
    FROM metrics
)
-- Final projection
SELECT
    id,
    days_since_release_date,
    duration_sec_change,
    next_bpm,
    bpm_quintile_min_next_bpm,
    running_bpm_quintile,
    second_duration_sec_change,
    running_bpm_quintile_minus_days_since_release_da,
    duration_sec_change_per_days_since_release_date,
    ROUND(days_since_release_date / NULLIF(next_bpm, 0), 3) AS days_since_release_date_per_next_bpm
FROM tagged
ORDER BY days_since_release_date_per_next_bpm DESC NULLS FIRST
