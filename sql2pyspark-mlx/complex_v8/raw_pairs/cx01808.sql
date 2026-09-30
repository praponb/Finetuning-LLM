WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        UPPER(TRIM(source)) AS source_clean,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE device IN ('speaker', 'phone', 'desktop')
),
without_matches AS (
    -- Keep rows whose track matches
    SELECT s.*
    FROM valid_plays s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.id = s.track_id
              AND t.is_explicit = TRUE
              AND ARRAY_CONTAINS(t.moods, 'workout')
        )
),
latest_per_key AS (
    -- Keep the earliest row per device
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        device_or_unknown,
        source_clean,
        device_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device ORDER BY device_len, id) AS recency_rank
        FROM without_matches
    ) ranked
    WHERE recency_rank = 1
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM latest_per_key
    WHERE ms_played > (SELECT AVG(ms_played) FROM latest_per_key)
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.device_or_unknown,
        s.source_clean,
        s.device_len,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source,
        p.device AS play_device
    FROM qualified s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    id,
    listener_id,
    ms_played,
    source,
    device_or_unknown,
    source_clean,
    device_len,
    play_id,
    play_is_skipped,
    play_source,
    play_device
FROM exploded_detail
ORDER BY play_source, play_device DESC, id, play_id
LIMIT 10
