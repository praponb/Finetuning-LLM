WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 0.9, 2) AS voltage_adj,
        LENGTH(TRIM(quality)) AS quality_len,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM meter_readings
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(quality_len) OVER (ORDER BY voltage_adj DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS quality_len_avg_1_prior,
        ROUND(kwh - AVG(kwh) OVER w_site_id_read_at_year, 2) AS kwh_vs_site_id_read_at_year_avg
    FROM cleaned
    WINDOW w_site_id_read_at_year AS (PARTITION BY site_id, YEAR(read_at))
)
SELECT id, metric_name, amount
FROM (
    SELECT id, voltage, quality_len_avg_1_prior, kwh_vs_site_id_read_at_year_avg, kwh
    FROM ranked
) src
UNPIVOT (
    amount FOR metric_name IN (voltage, quality_len_avg_1_prior, kwh_vs_site_id_read_at_year_avg, kwh)
) unp
