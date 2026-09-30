WITH cleaned AS (
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
    WHERE bpm < 166
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ROW_NUMBER() OVER w_artist_id_is_explicit_by_bpm AS bpm_seq,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS duration_sec_share_of_artist_id_is_explicit,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec), 3) AS duration_sec_cume_dist,
        NTILE(10) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm, id) AS bpm_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date NULLS LAST, id), 3) AS release_date_cume_dist,
        NTH_VALUE(release_date, 2) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        DATEDIFF(release_date, LAG(release_date) OVER (ORDER BY duration_sec, id)) AS days_since_prev_release_date,
        LEAD(bpm) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id) AS next_bpm,
        COUNT(*) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_duration_sec,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, is_explicit), 2) AS artist_id_is_explicit_avg_bpm,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id
    FROM cleaned
    WINDOW w_artist_id_is_explicit_by_bpm AS (PARTITION BY artist_id, is_explicit ORDER BY bpm, id), w_artist_id AS (PARTITION BY artist_id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec_share_of_artist_id_is_explicit,
        bpm_vs_artist_id_avg,
        duration_sec_cume_dist,
        bpm_decile,
        release_date_cume_dist,
        second_release_date,
        days_since_prev_release_date,
        next_bpm,
        running_count,
        artist_id_sum_duration_sec,
        artist_id_is_explicit_avg_bpm,
        rows_in_artist_id
    FROM metrics
    WHERE bpm_seq <= 3
      AND second_release_date < DATE '2025-04-02'
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm_vs_artist_id_avg,
        release_date_cume_dist,
        second_release_date,
        days_since_prev_release_date,
        next_bpm,
        artist_id_sum_duration_sec,
        rows_in_artist_id,
        ROUND(duration_sec_cume_dist / NULLIF(SUM(duration_sec_cume_dist) OVER w_bpm_decile_is_explicit, 0), 4) AS duration_sec_cume_dist_share_of_bpm_decile_is_ex,
        ROUND(next_bpm - AVG(next_bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', second_release_date), MONTH(release_date)), 2) AS next_bpm_vs_second_release_date_month_release_da,
        MIN(bpm_vs_artist_id_avg) OVER (ORDER BY running_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_vs_artist_id_avg_so_far,
        LAST_VALUE(next_bpm) IGNORE NULLS OVER (ORDER BY rows_in_artist_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_bpm
    FROM leaders
    WINDOW w_bpm_decile_is_explicit AS (PARTITION BY bpm_decile, is_explicit)
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        CASE WHEN rows_in_artist_id >= 14 THEN 'large' WHEN rows_in_artist_id >= 8 THEN 'medium' ELSE 'small' END AS rows_in_artist_id_tier,
        SUM(duration_sec) AS total_duration_sec,
        ROUND(AVG(next_bpm), 2) AS avg_next_bpm,
        MIN(days_since_prev_release_date) AS min_days_since_prev_release_date
    FROM metrics_2
    GROUP BY DATE_TRUNC('MONTH', release_date), CASE WHEN rows_in_artist_id >= 14 THEN 'large' WHEN rows_in_artist_id >= 8 THEN 'medium' ELSE 'small' END
)
SELECT
    release_date_month,
    rows_in_artist_id_tier,
    min_days_since_prev_release_date,
    LENGTH(TRIM(rows_in_artist_id_tier)) AS rows_in_artist_id_tier_len,
    CONCAT(rows_in_artist_id_tier, '#', CAST(total_duration_sec AS STRING)) AS rows_in_artist_id_tier_ref
FROM stats
WHERE total_duration_sec > 698
ORDER BY rows_in_artist_id_tier_ref DESC
