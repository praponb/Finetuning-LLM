WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
),
without_matches AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.birth_year >= 1966
        )
),
track_id_stats AS (
    -- Avg ms_played per track_id
    SELECT track_id, AVG(ms_played) AS avg_ms_played
    FROM without_matches
    GROUP BY track_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM without_matches s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.ms_played > g.avg_ms_played
)
SELECT
    id,
    listener_id,
    is_skipped,
    source,
    source_len,
    avg_ms_played,
    ms_played_minus_avg
FROM outliers
ORDER BY source DESC NULLS FIRST
