WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage >= 214.3
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(voltage) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY voltage NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_2_prior,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id,
        SUM(voltage) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY voltage NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS voltage_sum_3_prior,
        NTILE(4) OVER w_read_at_year_site_id_by_volt_3 AS voltage_quartile,
        ROUND(voltage - AVG(voltage) OVER w_quality_site_id, 2) AS voltage_vs_quality_site_id_avg,
        RANK() OVER (ORDER BY voltage) AS voltage_rank,
        LAST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY voltage, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_quality_by_read_at)) / 60, 1) AS minutes_since_prev_read_at_2,
        SUM(voltage) OVER (PARTITION BY quality, site_id) AS quality_site_id_sum_voltage,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY voltage NULLS LAST, id))) / 60, 1) AS minutes_since_prev_read_at_3
    FROM scoped_readings
    WINDOW w_read_at_year_site_id_by_volt_3 AS (PARTITION BY YEAR(read_at), site_id ORDER BY voltage NULLS LAST, id), w_quality_site_id AS (PARTITION BY quality, site_id), w_quality_by_read_at AS (PARTITION BY quality ORDER BY read_at DESC, id)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_slug,
        voltage_avg_2_prior,
        rows_in_quality_site_id,
        voltage_sum_3_prior,
        voltage_quartile,
        voltage_vs_quality_site_id_avg,
        last_known_voltage,
        minutes_since_prev_read_at,
        minutes_since_prev_read_at_2,
        quality_site_id_sum_voltage,
        minutes_since_prev_read_at_3
    FROM ranked
    WHERE voltage_rank <= 5
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    quality_slug,
    rows_in_quality_site_id,
    voltage_quartile,
    voltage_vs_quality_site_id_avg,
    last_known_voltage,
    minutes_since_prev_read_at,
    quality_site_id_sum_voltage,
    minutes_since_prev_read_at_3,
    ROUND(voltage * 0.9, 2) AS voltage_adj,
    CONCAT(quality_slug, '#', CAST(rows_in_quality_site_id AS STRING)) AS quality_slug_ref
FROM leaders
ORDER BY id, minutes_since_prev_read_at_3 DESC
