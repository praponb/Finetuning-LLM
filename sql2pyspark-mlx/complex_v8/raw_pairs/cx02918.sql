WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref
    FROM plays
    WHERE is_skipped = TRUE
),
with_parent AS (
    -- Join tracks
    SELECT s.*, t.moods AS track_moods
    FROM recent_plays s
    JOIN tracks t ON t.id = s.track_id
    WHERE ARRAY_CONTAINS(t.moods, 'sad')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(3) OVER w_source_by_ms_played AS ms_played_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id, source ORDER BY ms_played DESC, id), 3) AS ms_played_percent_rank,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY is_skipped)) AS distinct_listener_id_in_is_skipped,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source ORDER BY ms_played, id), 3) AS ms_played_percent_rank_2,
        LAST_VALUE(ms_played) OVER (PARTITION BY source ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY source ORDER BY ms_played, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY is_skipped)) AS distinct_track_id_in_is_skipped,
        ROW_NUMBER() OVER (PARTITION BY source ORDER BY ms_played, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id ORDER BY ms_played DESC NULLS FIRST, id), 3) AS ms_played_cume_dist,
        ms_played - LAG(ms_played) OVER (PARTITION BY source ORDER BY ms_played, id) AS ms_played_change,
        ROUND(CUME_DIST() OVER w_source_by_ms_played, 3) AS ms_played_cume_dist_2,
        ROUND(ms_played - AVG(ms_played) OVER w_is_skipped, 2) AS ms_played_vs_is_skipped_avg,
        LAG(ms_played, 1, 0) OVER (PARTITION BY source ORDER BY ms_played, id) AS prev_ms_played
    FROM with_parent
    WINDOW w_source_by_ms_played AS (PARTITION BY source ORDER BY ms_played, id), w_is_skipped AS (PARTITION BY is_skipped)
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    is_skipped,
    source,
    device_ref,
    ms_played_tercile,
    ms_played_percent_rank,
    distinct_listener_id_in_is_skipped,
    ms_played_percent_rank_2,
    last_ms_played,
    first_known_ms_played,
    ms_played_avg_2_prior,
    rn,
    ms_played_cume_dist,
    ms_played_change,
    ms_played_cume_dist_2,
    ms_played_vs_is_skipped_avg,
    prev_ms_played
FROM metrics
