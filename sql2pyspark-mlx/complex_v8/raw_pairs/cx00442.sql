WITH cleaned AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        CONCAT(plan, '#', CAST(id AS STRING)) AS plan_ref,
        COALESCE(birth_year, 0) AS birth_year_filled
    FROM listeners
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(birth_year_filled - 1994) AS birth_year_filled_distance,
        COALESCE(birth_year, 0) AS birth_year_filled_2,
        DATE_FORMAT(signup_date, 'MMM yyyy') AS signup_date_label
    FROM with_activity
    WHERE country = 'BR'
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.username,
        s.country,
        s.birth_year,
        s.birth_year_filled_distance,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.source AS play_source,
        p.ms_played AS play_ms_played
    FROM enriched s
    LEFT JOIN plays p ON p.listener_id = s.id AND p.device <> 'car'
),
per_group AS (
    -- Group-level totals
    SELECT
        play_device,
        play_id,
        DATE_FORMAT(play_played_at, 'yyyy-MM') AS play_played_at_ym,
        COUNT(*) AS cnt,
        ROUND(SUM(birth_year_filled_distance) / NULLIF(SUM(play_ms_played), 0), 3) AS birth_year_filled_distance_to_play_ms_played_rat,
        MAX(play_ms_played) AS max_play_ms_played
    FROM with_children
    GROUP BY 1, 2, 3
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN cnt >= 10 THEN 'A' WHEN cnt >= 6 THEN 'B' ELSE 'C' END AS cnt_tier
    FROM per_group
    WHERE play_device IS NOT NULL
),
tagged AS (
    -- Label and score rows
    SELECT *, LOWER(TRIM(cnt_tier)) AS cnt_tier_clean
    FROM enriched_2
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_device, play_id, cnt_tier, max_play_ms_played
    FROM tagged
) src
PIVOT (
    SUM(max_play_ms_played)
    FOR cnt_tier IN ('C', 'B', 'A')
)
ORDER BY play_device, play_id
