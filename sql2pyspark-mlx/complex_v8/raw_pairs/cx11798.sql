WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS('|', device, source) AS device_source_label,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        CONCAT_WS('-', device, source) AS device_source_label_2
    FROM plays
    WHERE source IN ('album', 'radio', 'search')
      AND track_id IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        ms_played,
        device,
        device_source_label_2,
        FIRST_VALUE(ms_played) OVER (ORDER BY is_skipped_flag ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        ROUND(PERCENT_RANK() OVER (ORDER BY is_skipped_flag ASC, id), 3) AS is_skipped_flag_percent_rank,
        ROUND(AVG(ms_played) OVER (ORDER BY ms_played ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_3_prior,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device), 2) AS ms_played_vs_device_avg,
        MAX(is_skipped_flag) OVER (PARTITION BY device) AS device_max_is_skipped_flag,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device
    FROM prepared
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        ms_played,
        device,
        first_ms_played,
        ms_played_avg_3_prior,
        ms_played_vs_device_avg,
        device_max_is_skipped_flag,
        LOWER(REGEXP_REPLACE(TRIM(device_source_label_2), ' ', '_')) AS device_source_label_2_slug,
        ROUND(ms_played_avg_3_prior * 1.1, 2) AS ms_played_avg_3_prior_adj,
        first_ms_played - ms_played_vs_device_avg AS first_ms_played_minus_ms_played_vs_device_avg
    FROM with_trends
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(first_ms_played) OVER (PARTITION BY device ORDER BY ms_played_vs_device_avg ASC, id) AS next_first_ms_played,
        LAST_VALUE(ms_played_avg_3_prior) OVER (PARTITION BY device ORDER BY ms_played_vs_device_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played_avg_3_prior,
        NTILE(4) OVER (PARTITION BY device ORDER BY ms_played_vs_device_avg ASC, id) AS ms_played_vs_device_avg_quartile
    FROM enriched
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT device_max_is_skipped_flag, ms_played_vs_device_avg_quartile, device, first_ms_played
    FROM with_trends_2
) src
PIVOT (
    COUNT(first_ms_played) AS count_v, SUM(first_ms_played) AS sum_v
    FOR device IN ('phone', 'tv', 'desktop')
)
