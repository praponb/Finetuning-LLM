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
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE HOUR(played_at) BETWEEN 9 AND 11
      AND listener_id IS NOT NULL
),
with_parent AS (
    -- Join listeners
    SELECT s.*, l.plan AS listener_plan, l.birth_year AS listener_birth_year
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_filled,
        listener_plan,
        listener_birth_year,
        ROW_NUMBER() OVER (ORDER BY listener_birth_year ASC, id) AS rn,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at), listener_id)) AS distinct_source_in_played_at_month_listener_id,
        SUM(listener_birth_year) OVER (PARTITION BY device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_listener_birth_year,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at), listener_id) AS rows_in_played_at_month_listener_id,
        ROUND(AVG(ms_played_filled) OVER (PARTITION BY listener_plan ORDER BY played_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_filled_avg_6_prior,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER w_listener_id, 0), 4) AS listener_birth_year_share_of_listener_id,
        NTILE(10) OVER w_listener_plan_by_played_at AS played_at_decile,
        ROUND(AVG(ms_played_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at), listener_id), 2) AS played_at_month_listener_id_avg_ms_played_filled
    FROM with_parent
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_listener_plan_by_played_at AS (PARTITION BY listener_plan ORDER BY played_at DESC, id)
)
-- Final projection
SELECT
    id,
    ms_played,
    device,
    is_skipped,
    source,
    ms_played_filled,
    listener_birth_year,
    distinct_source_in_played_at_month_listener_id,
    listener_birth_year_share_of_listener_id,
    played_at_decile,
    played_at_month_listener_id_avg_ms_played_filled,
    CASE WHEN listener_plan IN ('free', 'premium') THEN 'group_a' WHEN listener_plan = 'family' THEN 'group_b' ELSE 'other' END AS listener_plan_group
FROM ranked
ORDER BY played_at_month_listener_id_avg_ms_played_filled DESC, ms_played
