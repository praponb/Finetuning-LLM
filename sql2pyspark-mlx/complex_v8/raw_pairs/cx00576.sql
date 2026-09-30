WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 116.8
      AND site_id IS NOT NULL
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        si.tariff AS site_tariff,
        si.site_name
    FROM valid s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(voltage) OVER (PARTITION BY site_tariff, site_id ORDER BY voltage, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_sum_1_prior,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, site_tariff ORDER BY voltage, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_4,
        ROW_NUMBER() OVER (PARTITION BY site_id, site_tariff ORDER BY voltage, id) AS voltage_seq,
        MIN(voltage) OVER (PARTITION BY YEAR(read_at)) AS read_at_year_min_voltage,
        SUM(voltage) OVER (PARTITION BY YEAR(read_at)) AS read_at_year_sum_voltage,
        voltage - LAG(voltage) OVER (PARTITION BY site_tariff, site_id ORDER BY voltage, id) AS voltage_change,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_tariff), 2) AS voltage_vs_site_tariff_avg,
        SUM(voltage) OVER (PARTITION BY site_tariff, MONTH(read_at) ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS voltage_sum_3_prior
    FROM combined_details_site
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        read_at + INTERVAL 1 DAYS AS read_at_plus_1d
    FROM metrics
    WHERE site_id <= 4
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY site_id, quality ORDER BY voltage_avg_4, id) AS voltage_avg_4_decile,
        SUM(read_at_year_min_voltage) OVER (PARTITION BY quality_group, site_tariff ORDER BY voltage_change NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_read_at_year_min_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, quality ORDER BY voltage_avg_4, id), 3) AS voltage_avg_4_percent_rank,
        voltage_sum_1_prior - LAG(voltage_sum_1_prior) OVER w_quality_group_site_tariff_by_2 AS voltage_sum_1_prior_change,
        DENSE_RANK() OVER w_voltage_seq_quality_group_by AS read_at_year_sum_voltage_dense_rank
    FROM derived
    WINDOW w_quality_group_site_tariff_by_2 AS (PARTITION BY quality_group, site_tariff ORDER BY voltage_change NULLS LAST, id), w_voltage_seq_quality_group_by AS (PARTITION BY voltage_seq, quality_group ORDER BY read_at_year_sum_voltage DESC NULLS FIRST, id)
)
SELECT
    id,
    read_at,
    voltage,
    quality,
    site_tariff,
    site_name,
    voltage_sum_1_prior,
    voltage_avg_4,
    read_at_year_min_voltage,
    read_at_year_sum_voltage,
    voltage_change,
    voltage_vs_site_tariff_avg,
    quality_group,
    read_at_label,
    read_at_plus_1d,
    voltage_avg_4_decile,
    running_read_at_year_min_voltage,
    voltage_avg_4_percent_rank,
    voltage_sum_1_prior_change,
    read_at_year_sum_voltage_dense_rank,
    read_at + INTERVAL 1 DAYS AS read_at_plus_1d_2
FROM ranked
ORDER BY voltage_avg_4_percent_rank, read_at NULLS LAST, id
LIMIT 10
