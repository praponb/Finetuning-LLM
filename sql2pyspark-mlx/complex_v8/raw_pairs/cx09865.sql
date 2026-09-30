WITH base_plays AS (
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
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE listener_id IS NOT NULL
),
track_id_stats AS (
    -- Avg ms_played per track_id
    SELECT track_id, AVG(ms_played) AS avg_ms_played
    FROM base_plays
    GROUP BY track_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM base_plays s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.ms_played > g.avg_ms_played
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        ROUND(avg_ms_played / NULLIF(ms_played, 0), 3) AS avg_ms_played_per_ms_played
    FROM compared
    WHERE ms_played > 71476
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.first_played_at
    FROM labeled s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(play_count, 2) OVER (PARTITION BY play_count ORDER BY avg_ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_play_count,
        ROUND(AVG(ms_played_minus_avg) OVER (PARTITION BY play_count ORDER BY avg_ms_played DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_minus_avg_avg_2_prior
    FROM profiled
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(second_play_count) OVER (PARTITION BY device) AS device_sum_second_play_count,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        ROW_NUMBER() OVER (PARTITION BY second_play_count, listener_id ORDER BY avg_ms_played ASC NULLS LAST, id) AS rn
    FROM windowed
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    is_skipped,
    avg_ms_played,
    ms_played_minus_avg,
    device_or_unknown,
    avg_ms_played_per_ms_played,
    first_played_at,
    second_play_count,
    ms_played_minus_avg_avg_2_prior,
    SUBSTRING(TRIM(device), 1, 2) AS device_prefix
FROM windowed_2
ORDER BY listener_id DESC
