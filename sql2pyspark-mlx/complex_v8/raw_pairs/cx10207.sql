WITH recent_plays AS (
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
        CASE WHEN ms_played >= 236512 THEN 'A' WHEN ms_played >= 120663 THEN 'B' ELSE 'C' END AS ms_played_tier,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        ABS(ms_played - 183484) AS ms_played_distance
    FROM plays
    WHERE ms_played <= 218129
),
combined_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.source_prefix,
        s.ms_played_distance,
        t.title AS track_title,
        t.moods AS track_moods,
        t.release_date AS track_release_date
    FROM recent_plays s
    JOIN tracks t ON t.id = s.track_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        source,
        ARRAY_SORT(COLLECT_SET(track_title)) AS track_title_list,
        MAX(ms_played_distance) AS max_ms_played_distance,
        SUM(ms_played_distance) AS total_ms_played_distance,
        ROUND(STDDEV(ms_played), 2) AS stddev_ms_played
    FROM combined_details_track
    GROUP BY 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(total_ms_played_distance) OVER w_source AS source_sum_total_ms_played_distance,
        ROUND(CUME_DIST() OVER (ORDER BY total_ms_played_distance, source), 3) AS total_ms_played_distance_cume_dist,
        ROUND(total_ms_played_distance - AVG(total_ms_played_distance) OVER (PARTITION BY source), 2) AS total_ms_played_distance_vs_source_avg
    FROM per_group
    WINDOW w_source AS (PARTITION BY source)
)
SELECT
    source,
    max_ms_played_distance,
    total_ms_played_distance,
    stddev_ms_played,
    source_sum_total_ms_played_distance,
    total_ms_played_distance_cume_dist,
    total_ms_played_distance_vs_source_avg
FROM with_trends
ORDER BY source DESC, total_ms_played_distance DESC NULLS FIRST
