WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        ABS(ms_played - 225733) AS ms_played_distance,
        CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref
    FROM plays
    WHERE listener_id IS NOT NULL
      AND NOT is_skipped
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY source, is_skipped ORDER BY ms_played_distance ASC, id) AS ms_played_distance_rank,
        ROUND(ms_played_distance - AVG(ms_played_distance) OVER (PARTITION BY is_skipped, track_id), 2) AS ms_played_distance_vs_is_skipped_track_id_avg
    FROM cleaned
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        device,
        is_skipped,
        source,
        ms_played_distance,
        ms_played_distance_rank,
        ms_played_distance_vs_is_skipped_track_id_avg,
        ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played_distance_vs_is_skipped_track_id_avg, id) AS ms_played_distance_vs_is_skipped_track_id_avg_se,
        LAG(ms_played_distance_rank) OVER (PARTITION BY listener_id ORDER BY ms_played_distance_vs_is_skipped_track_id_avg, id) AS prev_ms_played_distance_rank,
        ms_played_distance_rank - LAG(ms_played_distance_rank) OVER w_ms_played_distance_rank_sour AS ms_played_distance_rank_change,
        ms_played_distance - LAG(ms_played_distance) OVER (PARTITION BY ms_played_distance_rank, source ORDER BY ms_played_distance_vs_is_skipped_track_id_avg DESC, id) AS ms_played_distance_change,
        ROUND(CUME_DIST() OVER (PARTITION BY ms_played_distance_rank, device ORDER BY ms_played_distance_rank DESC NULLS FIRST, id), 3) AS ms_played_distance_rank_cume_dist
    FROM windowed
    WINDOW w_ms_played_distance_rank_sour AS (PARTITION BY ms_played_distance_rank, source ORDER BY ms_played_distance_vs_is_skipped_track_id_avg DESC, id)
),
device_stats AS (
    -- Avg ms_played_distance_vs_is_skipped_track_id_avg_se per device
    SELECT device, AVG(ms_played_distance_vs_is_skipped_track_id_avg_se) AS avg_ms_played_distance_vs_is_skipped_track_id_av
    FROM metrics
    GROUP BY device
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played_distance_vs_is_skipped_track_id_av,
        ROUND(s.ms_played_distance_vs_is_skipped_track_id_avg_se - g.avg_ms_played_distance_vs_is_skipped_track_id_av, 2) AS ms_played_distance_vs_is_skipped_track_id_avg_se_2
    FROM metrics s
    JOIN device_stats g ON g.device = s.device
    WHERE s.ms_played_distance_vs_is_skipped_track_id_avg_se > g.avg_ms_played_distance_vs_is_skipped_track_id_av
),
unique_rows AS (
    -- Keep the earliest row per listener_id
    SELECT
        id,
        listener_id,
        device,
        is_skipped,
        source,
        ms_played_distance,
        ms_played_distance_rank,
        ms_played_distance_vs_is_skipped_track_id_avg,
        ms_played_distance_vs_is_skipped_track_id_avg_se,
        prev_ms_played_distance_rank,
        ms_played_distance_rank_change,
        ms_played_distance_change,
        ms_played_distance_rank_cume_dist,
        avg_ms_played_distance_vs_is_skipped_track_id_av,
        ms_played_distance_vs_is_skipped_track_id_avg_se_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played_distance_rank, id) AS recency_rank
        FROM vs_group
    ) ranked
    WHERE recency_rank = 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(prev_ms_played_distance_rank) OVER (PARTITION BY ms_played_distance_rank ORDER BY ms_played_distance, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_ms_played_distance_rank_sum_1_prior,
        LAG(ms_played_distance, 3) OVER w_ms_played_distance_rank_by_m_2 AS prev3_ms_played_distance,
        COUNT(*) OVER (PARTITION BY is_skipped, source) AS rows_in_is_skipped_source,
        ROW_NUMBER() OVER (PARTITION BY ms_played_distance_rank ORDER BY ms_played_distance, id) AS ms_played_distance_seq
    FROM unique_rows
    WINDOW w_ms_played_distance_rank_by_m_2 AS (PARTITION BY ms_played_distance_rank ORDER BY ms_played_distance, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        ms_played_distance_vs_is_skipped_track_id_avg_se,
        is_skipped,
        device,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        SUM(CASE WHEN is_skipped = FALSE THEN ms_played_distance_rank ELSE 0 END) AS ms_played_distance_rank_is_skipped_false
    FROM with_windows
    WHERE ms_played_distance_vs_is_skipped_track_id_avg_se_2 > -3.0
    GROUP BY ms_played_distance_vs_is_skipped_track_id_avg_se, is_skipped, device
)
SELECT
    ms_played_distance_vs_is_skipped_track_id_avg_se,
    is_skipped,
    source_list,
    ms_played_distance_rank_is_skipped_false,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label,
    COALESCE(device, 'UNKNOWN') AS device_or_unknown
FROM per_group
WHERE SIZE(source_list) >= 1
ORDER BY is_skipped_label
