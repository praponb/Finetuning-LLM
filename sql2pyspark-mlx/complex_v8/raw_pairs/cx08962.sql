WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-12 08:00:00'
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM filtered_readings s
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        kwh,
        voltage,
        quality,
        ABS(kwh - 63.6) AS kwh_distance,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM with_stats
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY voltage NULLS LAST) AS voltage_dense_rank,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality), 2) AS voltage_vs_quality_avg,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY quality ORDER BY kwh_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        ROUND(kwh_bucket / NULLIF(SUM(kwh_bucket) OVER (PARTITION BY quality), 0), 4) AS kwh_bucket_share_of_quality,
        ROUND(kwh_distance / NULLIF(SUM(kwh_distance) OVER w_quality, 0), 4) AS kwh_distance_share_of_quality
    FROM derived
    WINDOW w_quality AS (PARTITION BY quality)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        voltage,
        quality,
        kwh_distance,
        kwh_bucket,
        kwh_distance_share_of_quality,
        DENSE_RANK() OVER w_voltage_dense_rank_quality_b AS voltage_dense_rank_2,
        ROUND(AVG(voltage_dense_rank) OVER w_quality_voltage_dense_rank, 2) AS quality_voltage_dense_rank_avg_voltage_dense_ran,
        COUNT(*) OVER (PARTITION BY quality, voltage_dense_rank) AS rows_in_quality_voltage_dense_rank,
        RANK() OVER (PARTITION BY voltage_dense_rank, quality ORDER BY last_known_voltage DESC, id) AS last_known_voltage_rank,
        ROW_NUMBER() OVER (PARTITION BY voltage_dense_rank, quality ORDER BY voltage ASC, id) AS seq_no
    FROM with_windows
    WINDOW w_voltage_dense_rank_quality_b AS (PARTITION BY voltage_dense_rank, quality ORDER BY voltage ASC, id), w_quality_voltage_dense_rank AS (PARTITION BY quality, voltage_dense_rank)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(voltage, 0.0) AS voltage_filled,
        GREATEST(voltage_dense_rank_2, kwh_distance) AS greatest_voltage_dense_rank_2_kwh_distance,
        ROUND(voltage * 0.25, 2) AS voltage_adj
    FROM windowed
    WHERE kwh_distance_share_of_quality BETWEEN 0.2 AND 0.7
)
SELECT
    id,
    quality,
    kwh_distance,
    kwh_distance_share_of_quality,
    voltage_dense_rank_2,
    quality_voltage_dense_rank_avg_voltage_dense_ran,
    rows_in_quality_voltage_dense_rank,
    last_known_voltage_rank,
    seq_no,
    voltage_filled,
    greatest_voltage_dense_rank_2_kwh_distance,
    LOWER(TRIM(quality)) AS quality_clean,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
FROM tagged
WHERE voltage > 220.2
ORDER BY rows_in_quality_voltage_dense_rank
