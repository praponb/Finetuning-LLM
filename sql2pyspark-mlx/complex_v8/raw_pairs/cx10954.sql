WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 246779 THEN 'HIGH' WHEN ms_played >= 73177 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier
    FROM plays
    WHERE track_id >= 11
),
combined_details AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_tier,
        t.bpm AS track_bpm,
        t.title AS track_title
    FROM filtered_plays s
    JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(track_bpm) OVER (PARTITION BY device, track_id ORDER BY track_bpm, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS track_bpm_max_6_prior,
        DENSE_RANK() OVER (PARTITION BY track_id, ms_played_tier ORDER BY track_bpm ASC) AS track_bpm_dense_rank,
        MIN(track_bpm) OVER w_source_ms_played_tier AS source_ms_played_tier_min_track_bpm,
        ROUND(CUME_DIST() OVER (PARTITION BY device, track_id ORDER BY track_bpm), 3) AS track_bpm_cume_dist,
        FIRST_VALUE(track_bpm) OVER (PARTITION BY device, track_id ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_track_bpm,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY source, ms_played_tier)) AS distinct_device_in_source_ms_played_tier,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY source, ms_played_tier)) AS distinct_track_id_in_source_ms_played_tier,
        COUNT(*) OVER (PARTITION BY source, ms_played_tier) AS rows_in_source_ms_played_tier
    FROM combined_details
    WINDOW w_source_ms_played_tier AS (PARTITION BY source, ms_played_tier)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY listener_id ORDER BY distinct_device_in_source_ms_played_tier DESC, id) AS distinct_device_in_source_ms_played_tier_quintil,
        ROUND(track_bpm_cume_dist - AVG(track_bpm_cume_dist) OVER w_is_skipped_source, 2) AS track_bpm_cume_dist_vs_is_skipped_source_avg,
        LAG(track_bpm_max_6_prior, 1, 0) OVER (PARTITION BY is_skipped, distinct_device_in_source_ms_played_tier ORDER BY track_bpm ASC NULLS LAST, id) AS prev_track_bpm_max_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id ORDER BY distinct_device_in_source_ms_played_tier DESC, id), 3) AS distinct_device_in_source_ms_played_tier_percent,
        ROUND(AVG(track_bpm) OVER (PARTITION BY is_skipped, source), 2) AS is_skipped_source_avg_track_bpm,
        SIZE(COLLECT_SET(track_id) OVER w_is_skipped_source) AS distinct_track_id_in_is_skipped_source,
        MIN(first_track_bpm) OVER (PARTITION BY is_skipped, source) AS is_skipped_source_min_first_track_bpm
    FROM metrics
    WINDOW w_is_skipped_source AS (PARTITION BY is_skipped, source)
)
SELECT
    id,
    device,
    source,
    ms_played_tier,
    track_bpm,
    track_title,
    track_bpm_max_6_prior,
    track_bpm_dense_rank,
    source_ms_played_tier_min_track_bpm,
    first_track_bpm,
    distinct_device_in_source_ms_played_tier,
    rows_in_source_ms_played_tier,
    prev_track_bpm_max_6_prior,
    distinct_device_in_source_ms_played_tier_percent,
    distinct_track_id_in_is_skipped_source,
    is_skipped_source_min_first_track_bpm,
    CASE WHEN track_bpm_max_6_prior >= 372 THEN 'HIGH' WHEN track_bpm_max_6_prior >= 228 THEN 'MEDIUM' ELSE 'LOW' END AS track_bpm_max_6_prior_tier
FROM ranked
WHERE source_ms_played_tier_min_track_bpm >= 582
