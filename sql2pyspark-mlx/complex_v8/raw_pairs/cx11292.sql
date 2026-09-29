WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-25 10:00:00'
      AND device <> 'desktop'
),
source_stats AS (
    -- Avg ms_played per source
    SELECT source, AVG(ms_played) AS avg_ms_played
    FROM valid_plays
    GROUP BY source
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM valid_plays s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played > g.avg_ms_played
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        ms_played,
        is_skipped,
        avg_ms_played,
        FIRST_VALUE(ms_played_minus_avg) OVER (ORDER BY avg_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played_minus_avg,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_id)) AS distinct_source_in_track_id,
        FIRST_VALUE(ms_played_minus_avg) IGNORE NULLS OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played_minus_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played_minus_avg,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        LEAD(ms_played_minus_avg, 2) OVER (ORDER BY avg_ms_played, id) AS next2_ms_played_minus_avg,
        ROW_NUMBER() OVER (ORDER BY avg_ms_played, id) AS avg_ms_played_seq
    FROM outliers
),
without_matches AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.plan IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    listener_id,
    is_skipped,
    first_ms_played_minus_avg,
    distinct_source_in_track_id,
    first_known_ms_played_minus_avg,
    rows_in_track_id,
    avg_ms_played_seq
FROM without_matches
ORDER BY id, first_ms_played_minus_avg DESC
