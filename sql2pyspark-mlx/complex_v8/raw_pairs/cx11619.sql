WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE device <> 'desktop'
      AND ms_played > 112786
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        l.interests AS listener_interests,
        l.plan AS listener_plan,
        l.country AS listener_country
    FROM prepared s
    JOIN listeners l ON l.id = s.listener_id
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.is_skipped = FALSE
        )
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        listener_interests,
        listener_country,
        CASE WHEN ms_played >= 261579 THEN 'A' WHEN ms_played >= 71476 THEN 'B' ELSE 'C' END AS ms_played_tier,
        INITCAP(TRIM(listener_country)) AS listener_country_clean
    FROM filtered
    WHERE track_id >= 14
)
SELECT *
FROM (
    SELECT listener_id, listener_country, ms_played
    FROM tagged
) src
PIVOT (
    SUM(ms_played)
    FOR listener_country IN ('TH', 'BR')
)
