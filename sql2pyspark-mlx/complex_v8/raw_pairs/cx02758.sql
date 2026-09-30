WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        CASE WHEN ms_played >= 282311 THEN 'HIGH' WHEN ms_played >= 160264 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE track_id IS NOT NULL
      AND device NOT IN ('phone', 'desktop')
),
enriched_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.device_slug,
        s.ms_played_tier,
        s.is_skipped_flag,
        l.username AS listener_username,
        l.interests AS listener_interests,
        l.plan AS listener_plan
    FROM filtered_plays s
    JOIN listeners l ON l.id = s.listener_id
),
per_group AS (
    -- Group-level totals
    SELECT
        ms_played_tier,
        listener_id,
        ROUND(100 * COUNT(CASE WHEN device IN ('desktop', 'car') THEN 1 END) / COUNT(*), 1) AS pct_device_in_desktop_car,
        MAX(is_skipped_flag) AS max_is_skipped_flag
    FROM enriched_listener
    WHERE played_at < TIMESTAMP '2025-03-02 10:00:00'
    GROUP BY 1, 2
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.ms_played_tier,
        s.listener_id,
        s.pct_device_in_desktop_car,
        l.birth_year AS listener_birth_year
    FROM per_group s
    JOIN listeners l ON l.id = s.listener_id AND l.birth_year >= 1968
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at
    FROM joined s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        ms_played_tier,
        listener_id,
        pct_device_in_desktop_car,
        last_played_at,
        SUM(listener_birth_year) OVER (PARTITION BY TO_DATE(last_played_at) ORDER BY last_played_at DESC, ms_played_tier, listener_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_listener_birth_year,
        RANK() OVER (PARTITION BY TO_DATE(last_played_at) ORDER BY last_played_at DESC) AS last_played_at_rank,
        ROW_NUMBER() OVER (PARTITION BY ms_played_tier, listener_id ORDER BY listener_birth_year ASC, ms_played_tier, listener_id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ms_played_tier, listener_id ORDER BY listener_birth_year ASC), 3) AS listener_birth_year_percent_rank,
        ROUND(AVG(listener_birth_year) OVER (PARTITION BY ms_played_tier, listener_id), 2) AS ms_played_tier_listener_id_avg_listener_birth_ye
    FROM with_counts
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_played_at, ms_played_tier, ms_played_tier_listener_id_avg_listener_birth_ye
    FROM ranked
) src
PIVOT (
    COUNT(ms_played_tier_listener_id_avg_listener_birth_ye)
    FOR ms_played_tier IN ('HIGH', 'MEDIUM', 'LOW')
)
ORDER BY last_played_at
