WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean,
        UPPER(TRIM(quality)) AS quality_clean_2,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
    WHERE (quality IS NOT NULL OR voltage > 221.4)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN quality IN ('EST', 'OK') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM cleaned
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id, quality), 2) AS voltage_vs_site_id_quality_avg,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_site_id_quality, 0), 4) AS kwh_share_of_site_id_quality,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY quality_group ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        COUNT(*) OVER (PARTITION BY quality_group ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(kwh_filled) IGNORE NULLS OVER (PARTITION BY quality_group ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh_filled,
        SUM(kwh_filled) OVER w_site_id_quality AS site_id_quality_sum_kwh_filled
    FROM annotated
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        quality_clean,
        quality_clean_2,
        quality_prefix,
        kwh_filled,
        voltage_vs_site_id_quality_avg,
        second_voltage,
        running_count,
        first_known_kwh_filled,
        site_id_quality_sum_kwh_filled,
        MAX(second_voltage) OVER (PARTITION BY site_id, quality_group) AS site_id_quality_group_max_second_voltage,
        ROUND(second_voltage / NULLIF(SUM(second_voltage) OVER (PARTITION BY site_id, quality_group), 0), 4) AS second_voltage_share_of_site_id_quality_group
    FROM ranked
)
-- Subtotals with ROLLUP
SELECT
    quality,
    site_id,
    SUM(CASE WHEN quality IN ('OK', 'EST') THEN 1 ELSE 0 END) AS n_quality_in_ok_est
FROM with_trends
GROUP BY ROLLUP(quality, site_id)
ORDER BY quality NULLS LAST, site_id NULLS LAST
