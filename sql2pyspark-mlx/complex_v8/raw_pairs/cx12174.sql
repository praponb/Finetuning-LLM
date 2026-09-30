WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM plays
    WHERE device = 'car'
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.source IN ('playlist', 'search', 'radio') THEN 1 END) FROM plays p WHERE p.listener_id = s.listener_id) AS play_source_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at
    FROM scoped s
),
latest AS (
    -- Keep the earliest row per track_id, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        played_at_label,
        play_source_count,
        last_played_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, listener_id ORDER BY play_source_count, id) AS row_num
        FROM summarized_rows
    ) ranked
    WHERE row_num = 1
),
unique_rows AS (
    -- Keep the earliest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        played_at_label,
        play_source_count,
        last_played_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY played_at, id) AS rn
        FROM latest
    ) ranked
    WHERE rn = 1
)
SELECT id, metric, amount
FROM (
    SELECT id, play_source_count, ms_played
    FROM unique_rows
) src
UNPIVOT (
    amount FOR metric IN (play_source_count, ms_played)
) unp
ORDER BY id, metric
