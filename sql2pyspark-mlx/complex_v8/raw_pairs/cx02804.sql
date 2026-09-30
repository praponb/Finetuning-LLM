WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE kwh < 111.9
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(CASE WHEN quality NOT IN ('BAD', 'OK') THEN 1 END) AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM filtered s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh,
        least_voltage_kwh,
        quality_ref,
        kwh_filled,
        NTILE(5) OVER w_reading_quality_count_by_rea AS reading_quality_count_quintile,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id, 0), 4) AS voltage_share_of_site_id
    FROM with_stats
    WINDOW w_reading_quality_count_by_rea AS (PARTITION BY reading_quality_count ORDER BY reading_quality_count NULLS LAST, id), w_site_id AS (PARTITION BY site_id)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY reading_quality_count_quintile ORDER BY least_voltage_kwh ASC NULLS LAST, id), 3) AS least_voltage_kwh_percent_rank,
        DENSE_RANK() OVER (PARTITION BY reading_quality_count_quintile ORDER BY least_voltage_kwh ASC NULLS LAST) AS least_voltage_kwh_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY reading_quality_count_quintile ORDER BY least_voltage_kwh ASC NULLS LAST), 3) AS least_voltage_kwh_percent_rank_2,
        COUNT(*) OVER (PARTITION BY reading_quality_count_quintile) AS rows_in_reading_quality_count_quintile,
        SUM(least_voltage_kwh) OVER (PARTITION BY reading_quality_count_quintile ORDER BY least_voltage_kwh ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_least_voltage_kwh
    FROM sequenced
)
-- Final projection
SELECT
    id,
    kwh,
    least_voltage_kwh,
    quality_ref,
    kwh_filled,
    reading_quality_count_quintile,
    voltage_share_of_site_id,
    least_voltage_kwh_percent_rank,
    least_voltage_kwh_percent_rank_2,
    running_least_voltage_kwh
FROM sequenced_2
