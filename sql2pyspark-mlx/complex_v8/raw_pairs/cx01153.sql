WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CONCAT(device, '#', CAST(id AS STRING)) AS device_ref,
        played_at + INTERVAL 2 HOURS AS played_at_plus_2h
    FROM plays
    WHERE ms_played > 72464
),
listener_id_stats AS (
    -- Avg ms_played per listener_id
    SELECT listener_id, AVG(ms_played) AS avg_ms_played
    FROM cleaned_plays
    GROUP BY listener_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM cleaned_plays s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played > g.avg_ms_played
),
deduped AS (
    -- Keep the latest row per listener_id, source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        device_ref,
        played_at_plus_2h,
        avg_ms_played,
        ms_played_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id, source ORDER BY played_at_plus_2h DESC, id DESC) AS recency_rank
        FROM compared
    ) ranked
    WHERE recency_rank = 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        device_ref,
        played_at_plus_2h,
        avg_ms_played,
        ms_played_minus_avg,
        MIN(ms_played) OVER (PARTITION BY track_id) AS track_id_min_ms_played,
        SUM(avg_ms_played) OVER w_track_id AS track_id_sum_avg_ms_played,
        LEAD(played_at, 2) OVER (ORDER BY ms_played, id) AS next2_played_at,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_id)) AS distinct_source_in_track_id,
        NTILE(3) OVER (ORDER BY ms_played, id) AS ms_played_tercile
    FROM deduped
    WINDOW w_track_id AS (PARTITION BY track_id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', played_at_plus_2h) AS played_at_plus_2h_month,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        ROUND(SUM(ms_played) / NULLIF(SUM(ms_played_tercile), 0), 3) AS ms_played_to_ms_played_tercile_ratio,
        ROUND(SUM(avg_ms_played), 2) AS total_avg_ms_played,
        MAX(next2_played_at) AS last_next2_played_at,
        SUM(ms_played) FILTER (WHERE is_skipped = TRUE) AS ms_played_is_skipped_true,
        MAX(played_at) AS last_played_at,
        SUM(ms_played_minus_avg) AS total_ms_played_minus_avg
    FROM sequenced
    WHERE avg_ms_played BETWEEN 64250.0 AND 242000.0
    GROUP BY DATE_TRUNC('MONTH', played_at_plus_2h), DATE_TRUNC('MONTH', played_at)
)
SELECT
    played_at_plus_2h_month,
    played_at_month,
    last_next2_played_at,
    ms_played_is_skipped_true,
    last_played_at,
    total_ms_played_minus_avg
FROM totals
ORDER BY last_played_at
