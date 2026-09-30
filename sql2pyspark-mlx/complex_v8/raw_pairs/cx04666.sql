WITH valid AS (
    -- Filter plays
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
    WHERE track_id <= 12
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, COUNT(*) AS play_count, COUNT(*) AS play_count_2
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_count_2, 0) AS play_count_2
    FROM valid s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT played_at, device, play_count
    FROM with_stats
) src
PIVOT (
    COUNT(play_count) AS count_v, SUM(play_count) AS sum_v
    FOR device IN ('speaker', 'car')
)
ORDER BY played_at
