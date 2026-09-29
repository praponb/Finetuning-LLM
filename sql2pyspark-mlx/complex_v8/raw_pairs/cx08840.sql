WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 10 AND 14
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.tariff AS site_tariff
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(least_voltage_kwh - AVG(least_voltage_kwh) OVER (PARTITION BY quality, site_id), 2) AS least_voltage_kwh_vs_quality_site_id_avg,
        FIRST_VALUE(read_at) OVER (ORDER BY least_voltage_kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        ROW_NUMBER() OVER (PARTITION BY site_tariff, quality ORDER BY read_at, id) AS rn,
        SUM(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_sum_kwh,
        SUM(kwh) OVER (PARTITION BY quality ORDER BY least_voltage_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        RANK() OVER w_site_tariff_quality_by_read AS read_at_rank,
        ROUND(AVG(least_voltage_kwh) OVER (ORDER BY least_voltage_kwh DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS least_voltage_kwh_avg_2_prior,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        ROUND(least_voltage_kwh / NULLIF(SUM(least_voltage_kwh) OVER (PARTITION BY site_id), 0), 4) AS least_voltage_kwh_share_of_site_id,
        ROUND(AVG(least_voltage_kwh) OVER (PARTITION BY quality ORDER BY least_voltage_kwh, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS least_voltage_kwh_avg_2_prior_2,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY least_voltage_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(kwh - AVG(kwh) OVER w_site_id, 2) AS kwh_vs_site_id_avg,
        least_voltage_kwh - LAG(least_voltage_kwh) OVER (PARTITION BY site_id, site_tariff ORDER BY read_at ASC, id) AS least_voltage_kwh_change
    FROM combined_details_site
    WINDOW w_site_tariff_quality_by_read AS (PARTITION BY site_tariff, quality ORDER BY read_at, id), w_site_id AS (PARTITION BY site_id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        least_voltage_kwh,
        site_tariff,
        least_voltage_kwh_vs_quality_site_id_avg,
        first_read_at,
        quality_site_id_sum_kwh,
        running_kwh,
        read_at_rank,
        least_voltage_kwh_avg_2_prior,
        rows_in_site_id,
        least_voltage_kwh_share_of_site_id,
        least_voltage_kwh_avg_2_prior_2,
        first_known_kwh,
        kwh_vs_site_id_avg,
        least_voltage_kwh_change
    FROM windowed
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    site_id,
    least_voltage_kwh,
    site_tariff,
    least_voltage_kwh_vs_quality_site_id_avg,
    first_read_at,
    quality_site_id_sum_kwh,
    running_kwh,
    read_at_rank,
    least_voltage_kwh_avg_2_prior_2,
    first_known_kwh,
    kwh_vs_site_id_avg,
    CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
FROM first_rows
ORDER BY quality_site_id_sum_kwh, first_known_kwh DESC, id
LIMIT 10
