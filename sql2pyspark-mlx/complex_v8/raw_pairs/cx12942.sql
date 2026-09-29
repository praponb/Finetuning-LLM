WITH filtered AS (
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
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE source = 'radio'
      AND listener_id IS NOT NULL
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
        )
),
with_details_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_filled,
        l.username AS listener_username,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year,
        t.release_date AS track_release_date,
        t.title AS track_title
    FROM qualified s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
    WHERE SIZE(l.interests) >= 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(listener_birth_year) OVER (ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_listener_birth_year_so_far,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY listener_id)) AS distinct_device_in_listener_id,
        ROUND(ms_played_filled / NULLIF(SUM(ms_played_filled) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_filled_share_of_listener_id,
        MIN(listener_birth_year) OVER w_listener_id AS listener_id_min_listener_birth_year
    FROM with_details_listener
    WINDOW w_listener_id AS (PARTITION BY listener_id)
),
summary AS (
    -- Summarise per group
    SELECT
        track_id,
        device,
        listener_id,
        DATEDIFF(MAX(track_release_date), MIN(track_release_date)) AS track_release_date_span_days,
        COUNT(DISTINCT device) AS distinct_devices,
        ARRAY_SORT(COLLECT_SET(listener_username)) AS listener_username_list,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        COUNT(DISTINCT track_id) AS distinct_track_ids
    FROM ranked
    WHERE ms_played_filled BETWEEN 123500 AND 261750
    GROUP BY track_id, device, listener_id
    HAVING SUM(ms_played_filled) > 103750
),
enriched_track AS (
    -- Enrich with dimension data
    SELECT
        s.track_id,
        s.device,
        s.listener_id,
        s.track_release_date_span_days,
        s.distinct_devices,
        s.distinct_listener_ids,
        s.distinct_track_ids,
        t.is_explicit AS track_is_explicit,
        t.duration_sec AS track_duration_sec,
        t.artist_id AS track_artist_id
    FROM summary s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.is_explicit = TRUE
)
-- Final projection
SELECT
    track_id,
    device,
    listener_id,
    distinct_devices,
    distinct_listener_ids,
    track_is_explicit,
    track_duration_sec,
    track_artist_id,
    LENGTH(TRIM(device)) AS device_len
FROM enriched_track
ORDER BY distinct_devices NULLS LAST
