WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source
    FROM plays
    WHERE device IN ('desktop', 'speaker')
      AND source = 'radio'
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.last_played_at
    FROM recent s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
unique_rows AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        play_count,
        last_played_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY play_count DESC, id DESC) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.play_count,
        l.plan AS listener_plan,
        l.username AS listener_username
    FROM unique_rows s
    LEFT JOIN listeners l ON l.id = s.listener_id
)
-- Subtotals with CUBE
SELECT
    COALESCE(listener_plan, 'ALL') AS listener_plan,
    is_skipped,
    listener_id,
    SUM(play_count) AS total_play_count
FROM enriched
GROUP BY CUBE(listener_plan, is_skipped, listener_id)
