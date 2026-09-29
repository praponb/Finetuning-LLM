WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh < 110.5
      AND voltage >= 222.3
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY voltage), 3) AS voltage_percent_rank,
        SUM(kwh) OVER w_quality_site_id AS quality_site_id_sum_kwh,
        LAG(voltage) OVER (PARTITION BY quality ORDER BY voltage, id) AS prev_voltage,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        ROUND(AVG(voltage) OVER w_quality_site_id, 2) AS quality_site_id_avg_voltage,
        MIN(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_kwh,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY quality ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY voltage, id) AS voltage_seq,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg,
        LAG(read_at) OVER w_quality_by_voltage AS prev_read_at,
        MAX(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality ORDER BY kwh, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_max_2_prior
    FROM filtered
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id), w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage ASC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(quality)) AS quality_clean,
        CONCAT(quality, '#', CAST(voltage_seq AS STRING)) AS quality_ref
    FROM sequenced
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_percent_rank,
        prev_voltage,
        quality_site_id_avg_voltage,
        voltage_seq,
        voltage_vs_site_id_avg,
        prev_read_at,
        quality_clean,
        MIN(voltage_vs_site_id_avg) OVER w_prev_read_at_to_date_read_at AS prev_read_at_to_date_read_at_year_min_voltage_vs,
        MIN(kwh_vs_site_id_avg) OVER w_prev_read_at_to_date_read_at AS prev_read_at_to_date_read_at_year_min_kwh_vs_sit,
        NTH_VALUE(prev_voltage, 2) OVER (ORDER BY quality_site_id_avg_voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_voltage
    FROM annotated
    WINDOW w_prev_read_at_to_date_read_at AS (PARTITION BY TO_DATE(prev_read_at), YEAR(read_at))
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    voltage_percent_rank,
    quality_site_id_avg_voltage,
    voltage_seq,
    voltage_vs_site_id_avg,
    prev_read_at,
    quality_clean,
    prev_read_at_to_date_read_at_year_min_voltage_vs,
    prev_read_at_to_date_read_at_year_min_kwh_vs_sit,
    second_prev_voltage,
    DAYOFWEEK(prev_read_at) AS prev_read_at_dayofweek
FROM metrics
ORDER BY voltage
