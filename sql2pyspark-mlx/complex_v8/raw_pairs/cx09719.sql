WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 245815 THEN 'HIGH' WHEN ms_played >= 112786 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier
    FROM plays
    WHERE source IN ('search', 'album', 'radio')
      AND listener_id <= 6
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.source IN ('radio', 'playlist', 'search')
              AND p.is_skipped = FALSE
        )
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM with_activity
    WHERE ms_played > (SELECT MIN(ms_played) FROM with_activity)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(ms_played_tier), ' ', '_')) AS ms_played_tier_slug
    FROM filtered
    WHERE source = 'radio'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.ms_played_tier_slug,
        l.plan AS listener_plan,
        l.interests AS listener_interests
    FROM derived s
    JOIN listeners l ON l.id = s.listener_id
),
per_label AS (
    -- One row per listener_interest
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.ms_played_tier_slug,
        s.listener_plan,
        t.listener_interest
    FROM combined_details s
    LATERAL VIEW EXPLODE(s.listener_interests) t AS listener_interest
)
-- Final projection
SELECT
    id,
    track_id,
    is_skipped,
    listener_plan,
    CONCAT(listener_plan, '#', CAST(track_id AS STRING)) AS listener_plan_ref
FROM per_label
ORDER BY track_id
