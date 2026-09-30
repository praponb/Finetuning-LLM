WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        TO_DATE(played_at) AS played_at_date,
        CASE WHEN source IN ('playlist', 'search') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group,
        CASE WHEN ms_played >= 225733 THEN 'HIGH' WHEN ms_played >= 73335 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier
    FROM plays
    WHERE is_skipped = FALSE
      AND listener_id <= 14
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) AS play_count,
        COUNT(CASE WHEN source = 'playlist' THEN 1 END) AS play_source_count,
        MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_source_count, 0) AS play_source_count,
        p.last_played_at
    FROM filtered s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(play_count) OVER (PARTITION BY DATE_TRUNC('MONTH', last_played_at), track_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_play_count,
        ROUND(AVG(play_source_count) OVER w_last_played_at_to_date_ms_pl, 2) AS last_played_at_to_date_ms_played_tier_avg_play_s,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(played_at), source_group ORDER BY played_at DESC, id), 3) AS played_at_cume_dist
    FROM summarized_rows
    WINDOW w_last_played_at_to_date_ms_pl AS (PARTITION BY TO_DATE(last_played_at), ms_played_tier)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_source_count, played_at_date, ms_played_tier, last_play_count
    FROM sequenced
) src
PIVOT (
    AVG(last_play_count) AS avg_v, COUNT(last_play_count) AS n
    FOR ms_played_tier IN ('HIGH', 'MEDIUM', 'LOW')
)
ORDER BY play_source_count, played_at_date
