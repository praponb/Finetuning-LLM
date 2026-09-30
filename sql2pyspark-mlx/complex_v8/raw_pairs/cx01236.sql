WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE is_skipped = FALSE
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        ABS(ms_played - 245877) AS ms_played_distance
    FROM prepared_plays
)
SELECT id, metric, metric_value
FROM (
    SELECT id, ms_played, is_skipped_flag, ms_played_distance
    FROM annotated
) src
UNPIVOT (
    metric_value FOR metric IN (ms_played, is_skipped_flag, ms_played_distance)
) unp
