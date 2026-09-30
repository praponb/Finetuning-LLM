WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE kwh BETWEEN 20.3 AND 87.1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY kwh_per_voltage DESC NULLS FIRST), 3) AS kwh_per_voltage_percent_rank,
        NTH_VALUE(read_at, 2) OVER (ORDER BY kwh_per_voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROUND(CUME_DIST() OVER (ORDER BY kwh_per_voltage DESC, id), 3) AS kwh_per_voltage_cume_dist,
        ROUND(voltage - AVG(voltage) OVER w_read_at_to_date, 2) AS voltage_vs_read_at_to_date_avg,
        MAX(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY kwh_per_voltage DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3,
        LAG(read_at) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY kwh_per_voltage DESC NULLS FIRST, id) AS prev_read_at
    FROM cleaned
    WINDOW w_read_at_to_date AS (PARTITION BY TO_DATE(read_at))
),
joined AS (
    -- Attach site attributes
    SELECT s.*, si.commissioned_on AS site_commissioned_on
    FROM with_windows s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    quality,
    site_id,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
    ROUND(SUM(voltage_vs_read_at_to_date_avg), 2) AS total_voltage_vs_read_at_to_date_avg,
    COUNT(*) AS n_rows
FROM joined
GROUP BY CUBE(quality, site_id)
