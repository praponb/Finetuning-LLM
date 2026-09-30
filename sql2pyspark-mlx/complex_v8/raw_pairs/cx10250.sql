WITH valid AS (
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
        UPPER(TRIM(source)) AS source_clean,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE source <> 'radio'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(track_id) OVER w_listener_id) AS distinct_track_id_in_listener_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source ORDER BY played_at DESC, id), 3) AS played_at_percent_rank
    FROM valid
    WINDOW w_listener_id AS (PARTITION BY listener_id)
)
SELECT id, measure, reading
FROM (
    SELECT id, ms_played, distinct_track_id_in_listener_id
    FROM with_windows
) src
UNPIVOT (
    reading FOR measure IN (ms_played AS m_ms_played, distinct_track_id_in_listener_id AS distinct_track_id_in_listener_id_v)
) unp
