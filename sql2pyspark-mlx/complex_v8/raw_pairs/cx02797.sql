WITH recent AS (
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
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 11:00:00'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        source,
        is_skipped,
        listener_id,
        COUNT(*) AS n_rows,
        ROUND(100 * COUNT(CASE WHEN device = 'car' THEN 1 END) / COUNT(*), 1) AS pct_device_car,
        ROUND(100 * COUNT(CASE WHEN NOT is_skipped THEN 1 END) / COUNT(*), 1) AS pct_not_is_skipped
    FROM recent
    GROUP BY source, is_skipped, listener_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(pct_device_car - 50.0) AS pct_device_car_distance,
        ABS(n_rows - 8) AS n_rows_distance,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM totals
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM annotated
    WHERE n_rows_distance > (SELECT MIN(n_rows_distance) FROM annotated)
)
-- Native SQL UNPIVOT (wide to long)
SELECT source, is_skipped, metric, metric_value
FROM (
    SELECT source, is_skipped, n_rows, n_rows_distance
    FROM screened
) src
UNPIVOT (
    metric_value FOR metric IN (n_rows AS m_n_rows, n_rows_distance AS n_rows_distance_v)
) unp
