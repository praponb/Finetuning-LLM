WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE (is_skipped = FALSE OR device <> 'tv')
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS total_play_ms_played
    FROM filtered s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_count, listener_id, device, ms_played
    FROM with_stats
) src
PIVOT (
    AVG(ms_played) AS avg_v, SUM(ms_played) AS sum_v
    FOR device IN ('speaker', 'tv')
)
ORDER BY play_count, listener_id
