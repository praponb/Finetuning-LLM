WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CASE WHEN ms_played >= 223262 THEN 'A' WHEN ms_played >= 157900 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM plays
    WHERE listener_id IS NOT NULL
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM prepared_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ms_played_tier,
        prev_played_at,
        SIZE(COLLECT_SET(ms_played_tier) OVER (PARTITION BY track_id, source)) AS distinct_ms_played_tier_in_track_id_source,
        NTILE(10) OVER w_ms_played_tier_played_at_mon AS starts_session_decile,
        ROW_NUMBER() OVER (PARTITION BY ms_played_tier, MONTH(played_at) ORDER BY starts_session, id) AS rn,
        MAX(ms_played) OVER w_track_id_source AS track_id_source_max_ms_played
    FROM sessionized
    WINDOW w_ms_played_tier_played_at_mon AS (PARTITION BY ms_played_tier, MONTH(played_at) ORDER BY starts_session, id), w_track_id_source AS (PARTITION BY track_id, source)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        prev_played_at,
        distinct_ms_played_tier_in_track_id_source,
        starts_session_decile,
        rn,
        track_id_source_max_ms_played,
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref,
        DATE_TRUNC('WEEK', played_at) AS played_at_week
    FROM with_windows
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    prev_played_at,
    distinct_ms_played_tier_in_track_id_source,
    starts_session_decile,
    rn,
    track_id_source_max_ms_played,
    played_at_week,
    DAYOFWEEK(prev_played_at) AS prev_played_at_dayofweek,
    ABS(rn - 7) AS rn_distance
FROM annotated
