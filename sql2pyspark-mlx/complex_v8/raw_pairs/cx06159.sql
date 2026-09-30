WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE played_at < TIMESTAMP '2025-03-08 13:00:00'
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        is_skipped,
        is_skipped_flag,
        played_at_date,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM filtered_plays
),
latest AS (
    -- Keep the earliest row per listener_id
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        is_skipped,
        is_skipped_flag,
        played_at_date,
        ms_played_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played_filled, id) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
deduped AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        is_skipped,
        is_skipped_flag,
        played_at_date,
        ms_played_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY played_at DESC, id DESC) AS row_num
        FROM latest
    ) ranked
    WHERE row_num = 1
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, ms_played_filled, is_skipped_flag
    FROM deduped
) src
UNPIVOT (
    metric_value FOR metric_name IN (ms_played_filled, is_skipped_flag)
) unp
