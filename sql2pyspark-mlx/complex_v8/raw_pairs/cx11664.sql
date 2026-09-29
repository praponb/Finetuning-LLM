WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        LOWER(TRIM(quality)) AS quality_clean,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage BETWEEN 213.5 AND 241.9
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        quality,
        quality_clean,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY read_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        RANK() OVER w_site_id_quality_by_read_at_y_2 AS read_at_year_rank,
        NTILE(4) OVER (PARTITION BY quality ORDER BY read_at ASC, id) AS read_at_quartile,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY read_at_year, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS kwh_max_3_prior,
        SUM(voltage) OVER w_read_at_year_month_quality AS read_at_year_month_quality_sum_voltage
    FROM valid_readings
    WINDOW w_site_id_quality_by_read_at_y_2 AS (PARTITION BY site_id, quality ORDER BY read_at_year DESC, id), w_read_at_year_month_quality AS (PARTITION BY DATE_TRUNC('MONTH', read_at_year), quality)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE read_at_year_month_quality_sum_voltage > (SELECT AVG(read_at_year_month_quality_sum_voltage) FROM metrics)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, read_at_year_month_quality_sum_voltage, kwh_max_3_prior
    FROM with_activity
) src
UNPIVOT (
    value FOR measure IN (read_at_year_month_quality_sum_voltage, kwh_max_3_prior)
) unp
ORDER BY id, measure
