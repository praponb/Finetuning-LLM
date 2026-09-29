WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix,
        ABS(ms_played - 176302) AS ms_played_distance
    FROM plays
    WHERE listener_id IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        device_prefix,
        ms_played_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source ORDER BY ms_played_distance DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
joined AS (
    -- Join tracks
    SELECT
        s.*,
        t.release_date AS track_release_date,
        t.is_explicit AS track_is_explicit
    FROM unique_rows s
    JOIN tracks t ON t.id = s.track_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM joined
    WHERE ms_played_distance > (SELECT AVG(ms_played_distance) FROM joined)
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(source, 'ALL') AS source,
    listener_id,
    SUM(CASE WHEN is_skipped = FALSE THEN 1 ELSE 0 END) AS n_is_skipped_false,
    MIN(track_release_date) AS first_track_release_date,
    SUM(CASE WHEN source = 'album' THEN ms_played_distance ELSE 0 END) AS ms_played_distance_source_album
FROM without_matches
GROUP BY GROUPING SETS ((source, listener_id), (source), (listener_id), ())
ORDER BY source NULLS LAST, listener_id NULLS LAST
