WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        DATE_TRUNC('QUARTER', played_at) AS played_at_quarter
    FROM plays
    WHERE is_skipped = TRUE
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(ms_played) OVER (PARTITION BY YEAR(played_at), track_id ORDER BY played_at_quarter DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_max_2_prior,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_device_source, 0), 4) AS ms_played_share_of_device_source,
        SUM(ms_played) OVER (PARTITION BY source, MONTH(played_at) ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        LAG(played_at, 2) OVER (ORDER BY played_at DESC, id) AS prev2_played_at,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device, source), 2) AS ms_played_vs_device_source_avg,
        ROW_NUMBER() OVER (ORDER BY played_at DESC, id) AS rn,
        ROUND(AVG(ms_played) OVER (PARTITION BY source ORDER BY played_at_quarter DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        COUNT(*) OVER (PARTITION BY listener_id, MONTH(played_at_quarter)) AS rows_in_listener_id_played_at_quarter_month,
        LAST_VALUE(ms_played) OVER (PARTITION BY source ORDER BY played_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        NTILE(5) OVER w_source_played_at_month_by_pl_2 AS played_at_quintile,
        ms_played - LAG(ms_played) OVER (PARTITION BY source ORDER BY played_at_quarter DESC, id) AS ms_played_change,
        ROUND(CUME_DIST() OVER (PARTITION BY source, MONTH(played_at) ORDER BY played_at ASC), 3) AS played_at_cume_dist,
        SUM(ms_played) OVER (ORDER BY played_at_quarter DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_sum_7,
        MIN(ms_played) OVER (ORDER BY played_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far
    FROM prepared
    WINDOW w_device_source AS (PARTITION BY device, source), w_source_played_at_month_by_pl_2 AS (PARTITION BY source, MONTH(played_at) ORDER BY played_at ASC, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', source, device) AS source_device_label,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(played_at_quarter)) / 3600, 1) AS hours_played_at_quarter_to_played_at,
        TO_DATE(prev2_played_at) AS prev2_played_at_date
    FROM with_trends
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    played_at_label,
    ms_played_share_of_device_source,
    prev2_played_at,
    ms_played_vs_device_source_avg,
    rn,
    rows_in_listener_id_played_at_quarter_month,
    last_ms_played,
    played_at_quintile,
    ms_played_change,
    played_at_cume_dist,
    ms_played_sum_7,
    prev2_played_at_date
FROM tagged
WHERE source IS NOT NULL
ORDER BY id, ms_played_vs_device_source_avg DESC
LIMIT 10
