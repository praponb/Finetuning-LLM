WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = TRUE
      AND device = 'tv'
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.is_skipped = TRUE) FROM plays p WHERE p.track_id = s.track_id) AS play_is_skipped_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM prepared_plays s
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.is_skipped = TRUE
              AND p.ms_played > 160264
        )
),
source_stats AS (
    -- Avg ms_played per source
    SELECT source, AVG(ms_played) AS avg_ms_played
    FROM with_activity
    GROUP BY source
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM with_activity s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played > g.avg_ms_played
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.track_id) AS avg_play_ms_played
    FROM vs_group s
)
SELECT id, metric, amount
FROM (
    SELECT id, play_count, play_is_skipped_count
    FROM summarized_rows
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (play_count AS play_count_v, play_is_skipped_count AS play_is_skipped_count_v)
) unp
ORDER BY id, metric
