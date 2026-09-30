WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 10:00:00'
      AND source = 'radio'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(ms_played) OVER (PARTITION BY device, listener_id ORDER BY ms_played NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_max_3,
        LAG(ms_played, 2) OVER (PARTITION BY is_skipped ORDER BY ms_played, id) AS prev2_ms_played
    FROM cleaned
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.ms_played >= 73625
        )
      AND s.prev2_ms_played < 290754
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(ms_played, 3) OVER (PARTITION BY is_skipped ORDER BY ms_played_max_3, id) AS next3_ms_played,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped ORDER BY ms_played_max_3, id), 3) AS ms_played_max_3_cume_dist
    FROM screened
)
SELECT *
FROM (
    SELECT listener_id, track_id, source, prev2_ms_played
    FROM metrics
) src
PIVOT (
    MAX(prev2_ms_played)
    FOR source IN ('search', 'playlist', 'album', 'radio')
)
ORDER BY listener_id, track_id
