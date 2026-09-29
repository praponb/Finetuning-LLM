WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
        ABS(ms_played - 223262) AS ms_played_distance,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE source = 'search'
      AND is_skipped = FALSE
),
deduped AS (
    -- Keep the latest row per device, source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        days_since_played_at,
        ms_played_distance,
        ms_played_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device, source ORDER BY days_since_played_at DESC, id DESC) AS recency_rank
        FROM prepared_plays
    ) ranked
    WHERE recency_rank = 1
),
with_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.days_since_played_at,
        s.ms_played_distance,
        s.ms_played_filled,
        l.country AS listener_country,
        l.plan AS listener_plan
    FROM deduped s
    JOIN listeners l ON l.id = s.listener_id AND l.country = 'BR'
    WHERE l.country IN ('TH', 'BR', 'KR')
),
summary AS (
    -- Summarise per group
    SELECT
        listener_plan,
        listener_country,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        SUM(ms_played_distance) AS total_ms_played_distance,
        ROUND(AVG(ms_played_filled), 2) AS avg_ms_played_filled,
        COUNT(*) FILTER (WHERE listener_plan NOT IN ('premium', 'family')) AS n_listener_plan_not_in_premium
    FROM with_details
    WHERE played_at >= TIMESTAMP '2025-01-19 13:00:00'
    GROUP BY listener_plan, listener_country, DATE_FORMAT(played_at, 'yyyy-MM')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(avg_ms_played_filled) OVER (PARTITION BY listener_plan, n_listener_plan_not_in_premium ORDER BY n_listener_plan_not_in_premium DESC, listener_plan, listener_country, played_at_ym ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS avg_ms_played_filled_avg_2,
        ROUND(AVG(total_ms_played_distance) OVER (PARTITION BY listener_plan), 2) AS listener_plan_avg_total_ms_played_distance,
        SIZE(COLLECT_SET(listener_country) OVER (PARTITION BY listener_plan)) AS distinct_listener_country_in_listener_plan
    FROM summary
)
SELECT
    listener_plan,
    listener_country,
    total_ms_played_distance,
    avg_ms_played_filled,
    listener_plan_avg_total_ms_played_distance,
    distinct_listener_country_in_listener_plan,
    total_ms_played_distance - avg_ms_played_filled AS total_ms_played_distance_minus_avg_ms_played_fil,
    total_ms_played_distance - avg_ms_played_filled AS total_ms_played_distance_minus_avg_ms_played_fil_2
FROM ranked
