WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
    WHERE (source <> 'album' OR is_skipped = FALSE)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        played_at,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM filtered_plays
),
enriched_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.source,
        s.days_since_played_at,
        s.played_at_label,
        l.signup_date AS listener_signup_date
    FROM labeled s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.signup_date IS NOT NULL
),
with_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.source,
        s.days_since_played_at,
        s.played_at_label,
        l.plan AS listener_plan,
        l.country AS listener_country
    FROM enriched_listener s
    JOIN listeners l ON l.id = s.listener_id AND SIZE(l.interests) >= 2
    WHERE l.country NOT IN ('JP', 'UK')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT listener_plan, listener_country, days_since_played_at
    FROM with_details
) src
PIVOT (
    COUNT(days_since_played_at)
    FOR listener_country IN ('KR', 'UK', 'BR', 'JP')
)
