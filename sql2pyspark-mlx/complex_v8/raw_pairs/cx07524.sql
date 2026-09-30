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
        TO_DATE(played_at) AS played_at_date,
        CASE WHEN ms_played >= 218129 THEN 'large' WHEN ms_played >= 166517 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM plays
),
joined AS (
    -- Join listeners
    SELECT s.*, l.username AS listener_username, t.is_explicit AS track_is_explicit
    FROM valid s
    JOIN listeners l ON l.id = s.listener_id AND YEAR(l.signup_date) = 2024
    LEFT JOIN tracks t ON t.id = s.track_id
    WHERE UPPER(TRIM(l.username)) LIKE 'G%'
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        played_at_date,
        ms_played_tier,
        played_at_label,
        listener_username,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        INITCAP(TRIM(source)) AS source_clean
    FROM joined
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(device_or_unknown), ' ', '_')) AS device_or_unknown_slug
    FROM labeled
    WHERE device_or_unknown = 'UNKNOWN'
),
labeled_2 AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(listener_username), ' ', '_')) AS listener_username_slug,
        DATE_TRUNC('WEEK', played_at_date) AS played_at_date_week
    FROM scored
    WHERE played_at_label IS NOT NULL
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM labeled_2 s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
        )
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at
    FROM qualified s
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    is_skipped,
    source,
    played_at_date,
    ms_played_tier,
    played_at_label,
    device_or_unknown,
    device_or_unknown_slug,
    play_count,
    LEAST(play_count, ms_played) AS least_play_count_ms_played,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label
FROM profiled
ORDER BY track_id, played_at_label NULLS LAST
