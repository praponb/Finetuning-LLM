WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len,
        TO_DATE(played_at) AS played_at_date,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix
    FROM plays
    WHERE (track_id <= 4 OR ms_played > 62294)
),
enriched_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.source_len,
        s.played_at_date,
        s.source_prefix,
        t.is_explicit AS track_is_explicit,
        t.title AS track_title,
        t.duration_sec AS track_duration_sec
    FROM cleaned s
    LEFT JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY source)) AS distinct_listener_id_in_source,
        DENSE_RANK() OVER (PARTITION BY source_len, track_id ORDER BY track_duration_sec DESC) AS track_duration_sec_dense_rank,
        ms_played - LAG(ms_played) OVER (PARTITION BY source_len, track_id ORDER BY track_duration_sec DESC, id) AS ms_played_change,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER (PARTITION BY source), 2) AS track_duration_sec_vs_source_avg,
        LAST_VALUE(played_at) OVER (PARTITION BY source_len, track_id ORDER BY track_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at
    FROM enriched_track
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source_len,
        s.played_at_date,
        s.source_prefix,
        s.track_title,
        s.track_duration_sec,
        s.distinct_listener_id_in_source,
        s.ms_played_change,
        s.track_duration_sec_vs_source_avg,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM ranked s
    LEFT JOIN plays p ON p.track_id = s.track_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(play_played_at, 2) OVER (ORDER BY track_duration_sec_vs_source_avg, id, play_id) AS prev2_play_played_at,
        DENSE_RANK() OVER (ORDER BY play_played_at DESC) AS play_played_at_dense_rank,
        ROUND(play_ms_played - AVG(play_ms_played) OVER (PARTITION BY track_id, play_source), 2) AS play_ms_played_vs_track_id_play_source_avg
    FROM with_children
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric, reading
FROM (
    SELECT id, play_id, play_played_at_dense_rank, source_len, play_ms_played, track_duration_sec
    FROM sequenced
) src
UNPIVOT (
    reading FOR metric IN (play_played_at_dense_rank, source_len, play_ms_played, track_duration_sec)
) unp
