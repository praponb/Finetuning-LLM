WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 12:00:00'
      AND device IN ('tv', 'desktop')
),
latest_per_key AS (
    -- Keep the latest row per track_id, source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        is_skipped_flag,
        source_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, source ORDER BY is_skipped_flag DESC, id DESC) AS recency_rank
        FROM prepared_plays
    ) ranked
    WHERE recency_rank = 1
),
combined_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source_ref,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan
    FROM latest_per_key s
    JOIN listeners l ON l.id = s.listener_id AND l.country IS NOT NULL
)
-- Final projection
SELECT
    id,
    listener_id,
    ms_played,
    is_skipped,
    source_ref,
    listener_signup_date,
    ABS(ms_played - 246779) AS ms_played_distance
FROM combined_details_listener
ORDER BY ms_played, id DESC
