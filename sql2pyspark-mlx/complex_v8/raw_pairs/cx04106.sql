WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source <> 'radio'
      AND device NOT IN ('phone', 'desktop')
),
deduped AS (
    -- Keep the latest row per track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY played_at DESC, id DESC) AS row_num
        FROM base_plays
    ) ranked
    WHERE row_num = 1
),
unique_rows AS (
    -- Keep the earliest row per source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source ORDER BY played_at, id) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
summary AS (
    -- Group-level totals
    SELECT
        track_id,
        device,
        COUNT(*) FILTER (WHERE source = 'album') AS n_source_album,
        ROUND(100 * COUNT(CASE WHEN is_skipped = TRUE THEN 1 END) / COUNT(*), 1) AS pct_is_skipped_true
    FROM unique_rows
    GROUP BY track_id, device
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS min_play_ms_played,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS first_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM summary s
),
joined_track AS (
    -- Attach track attributes
    SELECT
        s.track_id,
        s.device,
        s.n_source_album,
        s.pct_is_skipped_true,
        s.min_play_ms_played,
        s.play_count,
        t.bpm AS track_bpm
    FROM profiled s
    JOIN tracks t ON t.id = s.track_id
)
SELECT track_id, device, metric_name, amount
FROM (
    SELECT track_id, device, track_bpm, play_count
    FROM joined_track
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (track_bpm, play_count)
) unp
ORDER BY track_id, device, metric_name
