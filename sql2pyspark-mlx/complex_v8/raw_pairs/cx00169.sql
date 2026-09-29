WITH cleaned AS (
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
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM plays
    WHERE track_id IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        ms_played,
        device,
        is_skipped,
        source,
        played_at_label,
        ABS(ms_played - 112786) AS ms_played_distance
    FROM cleaned
    WHERE device IN ('car', 'speaker')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        device,
        is_skipped,
        source,
        played_at_label,
        ROUND(CUME_DIST() OVER (PARTITION BY source ORDER BY ms_played_distance DESC, id), 3) AS ms_played_distance_cume_dist,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        ROUND(ms_played - AVG(ms_played) OVER w_device, 2) AS ms_played_vs_device_avg,
        ms_played_distance - LAG(ms_played_distance) OVER (PARTITION BY source ORDER BY ms_played_distance DESC, id) AS ms_played_distance_change
    FROM derived
    WINDOW w_device AS (PARTITION BY device)
),
per_group AS (
    -- Summarise per group
    SELECT
        device,
        source,
        is_skipped,
        MIN(rows_in_device) AS min_rows_in_device,
        COUNT(DISTINCT device) AS distinct_devices,
        ROUND(SUM(rows_in_device) / NULLIF(SUM(ms_played_distance_change), 0), 3) AS rows_in_device_to_ms_played_distance_change_rati,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE source = 'playlist') AS n_source_playlist,
        SUM(rows_in_device) AS total_rows_in_device
    FROM metrics
    WHERE ms_played_vs_device_avg >= -320000.0
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(source, '#', CAST(min_rows_in_device AS STRING)) AS source_ref
    FROM per_group
    WHERE distinct_devices > 4
),
scored AS (
    -- Derive helper columns
    SELECT
        device,
        source,
        is_skipped,
        distinct_devices,
        cnt,
        n_source_playlist,
        total_rows_in_device,
        source_ref,
        min_rows_in_device + cnt AS min_rows_in_device_plus_cnt,
        CASE WHEN source IN ('album', 'playlist') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM annotated
),
source_group_stats AS (
    -- Median distinct_devices per source_group
    SELECT source_group, PERCENTILE_APPROX(distinct_devices, 0.5) AS median_distinct_devices
    FROM scored
    GROUP BY source_group
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_distinct_devices,
        ROUND(s.distinct_devices - g.median_distinct_devices, 2) AS distinct_devices_minus_median
    FROM scored s
    JOIN source_group_stats g ON g.source_group = s.source_group
    WHERE s.distinct_devices >= g.median_distinct_devices
)
-- Final projection
SELECT
    device,
    is_skipped,
    distinct_devices,
    cnt,
    n_source_playlist,
    total_rows_in_device,
    source_ref,
    min_rows_in_device_plus_cnt,
    source_group,
    median_distinct_devices,
    GREATEST(total_rows_in_device, n_source_playlist) AS greatest_total_rows_in_device_n_source_playlist,
    COALESCE(n_source_playlist, 0) AS n_source_playlist_filled
FROM above_average
ORDER BY source_group NULLS LAST
