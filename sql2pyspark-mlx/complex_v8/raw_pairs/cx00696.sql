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
        CONCAT_WS(' / ', device, source) AS device_source_label
    FROM plays
    WHERE NOT is_skipped
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY is_skipped), 0), 4) AS ms_played_share_of_is_skipped,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY track_id, device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played
    FROM valid
),
annotated AS (
    -- Derive helper columns
    SELECT *, ABS(second_ms_played - 236512) AS second_ms_played_distance
    FROM windowed
)
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    is_skipped,
    source,
    ms_played_share_of_is_skipped,
    rows_in_is_skipped,
    second_ms_played,
    second_ms_played_distance,
    ABS(second_ms_played - 195566) AS second_ms_played_distance_2
FROM annotated
