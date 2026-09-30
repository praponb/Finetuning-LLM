WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
    WHERE (ms_played <= 225733 OR played_at < TIMESTAMP '2025-03-02 09:00:00')
),
track_id_stats AS (
    -- Avg ms_played per track_id
    SELECT track_id, AVG(ms_played) AS avg_ms_played
    FROM scoped_plays
    GROUP BY track_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM scoped_plays s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.ms_played > g.avg_ms_played
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY listener_id, track_id ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        SUM(ms_played_minus_avg) OVER (PARTITION BY source) AS source_sum_ms_played_minus_avg
    FROM above_average
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MIN(ms_played) AS min_play_ms_played
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.min_play_ms_played
    FROM windowed s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
)
-- Subtotals with GROUPING SETS
SELECT
    device,
    source,
    ROUND(AVG(avg_ms_played), 2) AS avg_avg_ms_played,
    COUNT(*) AS n_rows,
    ARRAY_SORT(COLLECT_SET(source)) AS source_list
FROM summarized_rows
GROUP BY GROUPING SETS ((device, source), (device), (source), ())
ORDER BY device NULLS LAST, source NULLS LAST
