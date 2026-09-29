WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        CASE WHEN device IN ('tv', 'speaker') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-12 10:00:00'
      AND source = 'search'
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at
    FROM valid_plays s
    LEFT JOIN plays p ON p.track_id = s.track_id AND p.device <> 'speaker'
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        played_at,
        source,
        play_id,
        play_device,
        play_ms_played,
        play_played_at,
        COALESCE(play_source, 'UNKNOWN') AS play_source_or_unknown,
        LENGTH(TRIM(source)) AS source_len,
        DATEDIFF(DATE '2025-06-30', play_played_at) AS days_since_play_played_at
    FROM with_children
),
totals AS (
    -- Summarise per group
    SELECT
        play_id,
        DATE_TRUNC('MONTH', play_played_at) AS play_played_at_month,
        ROUND(AVG(source_len), 2) AS avg_source_len,
        SUM(play_ms_played) AS total_play_ms_played
    FROM labeled
    GROUP BY play_id, DATE_TRUNC('MONTH', play_played_at)
    HAVING SUM(play_ms_played) > 92277
),
annotated AS (
    -- Add calculated columns
    SELECT
        play_id,
        play_played_at_month,
        avg_source_len,
        ROUND(total_play_ms_played / NULLIF(avg_source_len, 0), 3) AS total_play_ms_played_per_avg_source_len,
        HOUR(play_played_at_month) AS play_played_at_month_hour,
        ROUND(total_play_ms_played / NULLIF(avg_source_len, 0), 3) AS total_play_ms_played_per_avg_source_len_2
    FROM totals
    WHERE total_play_ms_played > 843250
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(play_played_at_month_hour) OVER (PARTITION BY play_played_at_month_hour, play_id ORDER BY total_play_ms_played_per_avg_source_len, play_id, play_played_at_month ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS play_played_at_month_hour_avg_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(play_played_at_month) ORDER BY play_played_at_month_hour DESC), 3) AS play_played_at_month_hour_percent_rank,
        avg_source_len - LAG(avg_source_len) OVER (PARTITION BY play_played_at_month_hour, play_id ORDER BY total_play_ms_played_per_avg_source_len, play_id, play_played_at_month) AS avg_source_len_change
    FROM annotated
)
-- Final projection
SELECT DISTINCT play_id, play_played_at_month_hour_percent_rank, play_played_at_month
FROM with_windows
ORDER BY play_id NULLS LAST
