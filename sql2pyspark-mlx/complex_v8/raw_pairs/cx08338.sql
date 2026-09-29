WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        CASE WHEN ms_played >= 241585 THEN 'HIGH' WHEN ms_played >= 168782 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-19 11:00:00'
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MAX(played_at) AS last_played_at,
        SUM(ms_played) AS total_play_ms_played,
        MAX(ms_played) AS max_play_ms_played
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        p.total_play_ms_played,
        p.max_play_ms_played
    FROM recent s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(total_play_ms_played) OVER (ORDER BY played_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS total_play_ms_played_sum_6_prior,
        ROW_NUMBER() OVER (ORDER BY played_at DESC, id) AS played_at_seq,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY ms_played_tier)) AS distinct_listener_id_in_ms_played_tier,
        ROUND(AVG(max_play_ms_played) OVER (ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS max_play_ms_played_avg_3_prior
    FROM with_counts
),
without_matches AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.country IN ('KR', 'TH', 'UK')
        )
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        ms_played_tier,
        last_played_at,
        total_play_ms_played,
        max_play_ms_played,
        total_play_ms_played_sum_6_prior,
        distinct_listener_id_in_ms_played_tier,
        max_play_ms_played_avg_3_prior
    FROM without_matches
    WHERE played_at_seq = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT played_at, track_id, source, ms_played
    FROM first_rows
) src
PIVOT (
    MIN(ms_played)
    FOR source IN ('search', 'album', 'radio', 'playlist')
)
ORDER BY played_at, track_id
