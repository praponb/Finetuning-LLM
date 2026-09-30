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
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        CASE WHEN ms_played >= 221269 THEN 'A' WHEN ms_played >= 97825 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM plays
    WHERE is_skipped = TRUE
),
with_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        l.signup_date AS listener_signup_date,
        l.country AS listener_country,
        l.plan AS listener_plan,
        t.moods AS track_moods
    FROM prepared_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(listener_signup_date, 'yyyy-MM') AS listener_signup_date_label
    FROM with_details
)
-- Final projection
SELECT
    id,
    played_at,
    is_skipped,
    source,
    listener_signup_date,
    listener_country,
    listener_plan,
    track_moods,
    listener_signup_date_label,
    HOUR(played_at) AS played_at_hour
FROM labeled
