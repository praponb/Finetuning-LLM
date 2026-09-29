WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE (is_skipped = TRUE OR source IS NOT NULL)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(ms_played) OVER (PARTITION BY is_skipped, track_id) AS is_skipped_track_id_sum_ms_played,
        LAG(ms_played, 2) OVER (PARTITION BY source ORDER BY ms_played ASC, id) AS prev2_ms_played,
        ms_played - LAG(ms_played) OVER (PARTITION BY source ORDER BY ms_played ASC, id) AS ms_played_change
    FROM cleaned_plays
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
        )
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_change,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM with_activity
    WHERE listener_id <= 9
),
with_parent_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.ms_played_change,
        s.is_skipped_label,
        s.device_slug,
        t.duration_sec AS track_duration_sec,
        t.artist_id AS track_artist_id,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM labeled s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
)
-- Final projection
SELECT
    id,
    is_skipped,
    source,
    ms_played_change,
    is_skipped_label,
    device_slug,
    track_artist_id,
    artist_genre,
    GREATEST(ms_played_change, ms_played) AS greatest_ms_played_change_ms_played,
    LOWER(TRIM(device_slug)) AS device_slug_clean
FROM with_parent_track
ORDER BY greatest_ms_played_change_ms_played, device_slug DESC
