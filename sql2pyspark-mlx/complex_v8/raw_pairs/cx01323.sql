WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage
    FROM meter_readings
    WHERE kwh >= 26.9
      AND voltage BETWEEN 213.5 AND 232.0
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_quality_site_id AS rows_in_quality_site_id,
        ROUND(least_kwh_voltage - AVG(least_kwh_voltage) OVER w_quality_site_id, 2) AS least_kwh_voltage_vs_quality_site_id_avg,
        NTILE(5) OVER (PARTITION BY site_id ORDER BY least_kwh_voltage, id) AS least_kwh_voltage_quintile,
        MIN(voltage) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_voltage,
        LEAD(least_kwh_voltage) OVER (PARTITION BY site_id ORDER BY least_kwh_voltage, id) AS next_least_kwh_voltage,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality, site_id), 0), 4) AS voltage_share_of_quality_site_id
    FROM scoped
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id)
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM with_windows s
    LEFT JOIN sites si ON si.id = s.site_id AND si.commissioned_on IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY quality, site_zone) AS rows_in_quality_site_zone,
        ROUND(AVG(rows_in_quality_site_id) OVER w_quality_site_zone, 2) AS quality_site_zone_avg_rows_in_quality_site_id,
        DENSE_RANK() OVER (PARTITION BY least_kwh_voltage_quintile ORDER BY read_at, id) AS read_at_dense_rank,
        MIN(voltage) OVER (PARTITION BY quality, site_zone) AS quality_site_zone_min_voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY least_kwh_voltage_quintile ORDER BY read_at), 3) AS read_at_cume_dist,
        SUM(voltage) OVER (ORDER BY quality_site_id_min_voltage ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS voltage_sum_6_prior
    FROM joined_site
    WINDOW w_quality_site_zone AS (PARTITION BY quality, site_zone)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM ranked
    WHERE read_at_dense_rank <= 3
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    least_kwh_voltage,
    rows_in_quality_site_id,
    least_kwh_voltage_vs_quality_site_id_avg,
    least_kwh_voltage_quintile,
    quality_site_id_min_voltage,
    next_least_kwh_voltage,
    site_tariff,
    site_zone,
    site_commissioned_on,
    rows_in_quality_site_zone,
    quality_site_zone_avg_rows_in_quality_site_id,
    read_at_dense_rank,
    quality_site_zone_min_voltage,
    read_at_cume_dist,
    ABS(read_at_cume_dist - 0.3) AS read_at_cume_dist_distance,
    CONCAT(site_tariff, '#', CAST(read_at_dense_rank AS STRING)) AS site_tariff_ref
FROM leaders
ORDER BY site_zone DESC NULLS FIRST, least_kwh_voltage_quintile DESC
