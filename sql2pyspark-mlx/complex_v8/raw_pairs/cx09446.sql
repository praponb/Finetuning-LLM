WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
),
derived AS (
    -- Label and score rows
    SELECT *, ABS(voltage - 225.3) AS voltage_distance
    FROM prepared_readings
    WHERE quality NOT IN ('BAD', 'EST')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(voltage_distance - AVG(voltage_distance) OVER w_read_at_month, 2) AS voltage_distance_vs_read_at_month_avg,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_read_at_month, 0), 4) AS voltage_share_of_read_at_month,
        ROW_NUMBER() OVER (PARTITION BY quality, TO_DATE(read_at) ORDER BY kwh_bucket ASC, id) AS rn,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY voltage DESC, id) AS rn_2
    FROM derived
    WINDOW w_read_at_month AS (PARTITION BY DATE_TRUNC('MONTH', read_at))
)
SELECT
    quality,
    rn,
    site_id,
    COUNT(*) AS n_rows,
    SUM(CASE WHEN quality IN ('EST', 'OK') THEN voltage_share_of_read_at_month ELSE 0 END) AS voltage_share_of_read_at_month_quality_in_est_ok
FROM metrics
GROUP BY GROUPING SETS ((quality, rn, site_id), (quality))
ORDER BY quality NULLS LAST, rn NULLS LAST, site_id NULLS LAST
