WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 223828 THEN 'HIGH' WHEN ms_played >= 168782 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        ABS(ms_played - 145720) AS ms_played_distance,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE MONTH(played_at) IN (1, 3)
      AND track_id IS NOT NULL
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS max_play_ms_played,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at
    FROM valid_plays s
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at_2,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS max_play_ms_played_2
    FROM with_stats s
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        DATEDIFF(DATE '2026-01-01', last_played_at_2) AS days_since_last_played_at_2,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug_2
    FROM summarized_rows
    WHERE ms_played_distance <= 300000
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT listener_id, is_skipped, ms_played_tier, max_play_ms_played_2
    FROM scored
) src
PIVOT (
    SUM(max_play_ms_played_2)
    FOR ms_played_tier IN ('HIGH', 'LOW')
)
ORDER BY listener_id, is_skipped
