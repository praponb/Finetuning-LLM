WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE (quality IN ('BAD', 'OK') OR kwh < 109.0)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        voltage,
        quality,
        quality_or_unknown,
        quality_ref,
        NTILE(5) OVER (ORDER BY read_at, id) AS read_at_quintile,
        SUM(voltage) OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        DENSE_RANK() OVER (ORDER BY read_at) AS read_at_dense_rank
    FROM prepared
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM with_windows
    WHERE read_at_quintile < (SELECT MAX(read_at_quintile) FROM with_windows)
)
SELECT id, measure, metric_value
FROM (
    SELECT id, read_at_quintile, read_at_dense_rank
    FROM without_matches
) src
UNPIVOT (
    metric_value FOR measure IN (read_at_quintile, read_at_dense_rank)
) unp
