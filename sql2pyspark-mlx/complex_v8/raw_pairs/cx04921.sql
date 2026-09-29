WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM cleaned_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.ms_played >= 107912
        )
),
listener_id_stats AS (
    -- Max ms_played per listener_id
    SELECT listener_id, MAX(ms_played) AS max_ms_played
    FROM filtered
    GROUP BY listener_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played,
        ROUND(s.ms_played - g.max_ms_played, 2) AS ms_played_minus_max
    FROM filtered s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played = g.max_ms_played
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        max_ms_played,
        ms_played_minus_max,
        NTILE(3) OVER w_listener_id_track_id_by_play AS played_at_tercile,
        LAST_VALUE(played_at) OVER (PARTITION BY track_id, listener_id ORDER BY max_ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        RANK() OVER (PARTITION BY listener_id, track_id ORDER BY played_at ASC) AS played_at_rank,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY source), 2) AS ms_played_vs_source_avg,
        ms_played_minus_max - LAG(ms_played_minus_max) OVER w_all_by_played_at AS ms_played_minus_max_change
    FROM vs_group
    WINDOW w_listener_id_track_id_by_play AS (PARTITION BY listener_id, track_id ORDER BY played_at ASC, id), w_all_by_played_at AS (ORDER BY played_at DESC NULLS FIRST, id)
)
-- Subtotals with ROLLUP
SELECT
    played_at_rank,
    is_skipped,
    GROUPING(played_at_rank) AS is_total_played_at_rank,
    ARRAY_SORT(COLLECT_SET(device)) AS device_list
FROM windowed
GROUP BY ROLLUP(played_at_rank, is_skipped)
