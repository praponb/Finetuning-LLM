WITH valid_plays AS (
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
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE device IS NOT NULL
      AND is_skipped = TRUE
),
with_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.played_at_label,
        s.source_slug,
        t.is_explicit AS track_is_explicit,
        t.bpm AS track_bpm,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year
    FROM valid_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
exploded AS (
    -- One row per listener_interest
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.played_at_label,
        s.source_slug,
        s.track_is_explicit,
        s.track_bpm,
        s.listener_birth_year,
        t.listener_interest
    FROM with_details_track s
    LATERAL VIEW OUTER EXPLODE(s.listener_interests) t AS listener_interest
),
enriched AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.played_at_label,
        s.source_slug,
        s.track_bpm,
        s.listener_birth_year,
        s.listener_interest,
        l.username AS listener_username,
        l.plan AS listener_plan,
        l.birth_year AS listener_birth_year_2,
        t.duration_sec AS track_duration_sec
    FROM exploded s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
    WHERE l.birth_year > 1975
)
SELECT
    listener_plan,
    listener_id,
    track_id,
    MIN(played_at) AS first_played_at,
    ARRAY_SORT(COLLECT_SET(listener_username)) AS listener_username_list
FROM enriched
GROUP BY ROLLUP(listener_plan, listener_id, track_id)
ORDER BY listener_plan NULLS LAST, listener_id NULLS LAST, track_id NULLS LAST
